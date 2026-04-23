/*
 * Petrol Station Display — ESP32-WROOM-32
 *
 * Live UK fuel price display:
 *   - Waveshare 2" ST7789V 320x240 landscape LCD shows station name/location
 *   - Two 3-digit 7-seg modules (daisy-chained) show unleaded/diesel prices in ppl
 *
 * Data source: UK Fuel Finder API (OAuth2 client credentials).
 * Geocoding:   postcodes.io (free, no key).
 * Setup:       Captive portal on first boot (Wi-Fi + postcode + radius).
 *
 * Required libraries:
 *   - TFT_eSPI (Bodmer) with custom User_Setup.h
 *   - ArduinoJson v7+ (Benoit Blanchon)
 *   - WiFi, WebServer, DNSServer, Preferences, HTTPClient (built-in with ESP32 core)
 *
 * Board: ESP32 Dev Module
 *
 * Finds cheapest E10 (unleaded) station within user-specified radius of their
 * postcode, then displays:
 *   - that station's E10 price on the TOP 7-seg module (unleaded)
 *   - that station's B7 (diesel) price on the BOTTOM 7-seg module (diesel)
 *   - station name + town on the LCD
 *
 * Refreshes hourly. Falls back to last cached values if fetch fails.
 */

#include <Arduino.h>
#include <SPI.h>
#include <TFT_eSPI.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <FS.h>
using fs::FS;  // ESP32 core v3.x workaround
#include <WebServer.h>
#include <DNSServer.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <time.h>
#include <math.h>
#include <esp_task_wdt.h>

// =============================================================
// Type definitions (declared here so Arduino IDE's auto-generated
// function prototypes at the top of the translation unit see them.)
// =============================================================
struct GeoLocation {
  double lat;
  double lon;
  String town;
  bool valid;
};

struct NearbyStation {
  String siteId;
  String brand;
  String name;
  String town;
  double lat;
  double lon;
  double distanceMi;
  int priceE10_ppl;
  int priceB7_ppl;
};

// =============================================================
// CONFIG — edit these
// =============================================================

// Fuel Finder OAuth credentials (from developer.fuel-finder.service.gov.uk)
// NOTE: stored in firmware. Anyone with USB access can extract them.
const char* FUEL_FINDER_CLIENT_ID     = "fCmHTPxSUzvlYVgvGkbuplzZ01HN842U";
const char* FUEL_FINDER_CLIENT_SECRET = "40njh0w6EnbK7C2wXEjpwphGc4E8Tl0m3o4gNCZhF9bdZX2NK5SychA9EoUHp9Ab";

// API endpoints
const char* FUEL_FINDER_BASE = "https://www.fuel-finder.service.gov.uk";
const char* POSTCODES_IO_BASE = "https://api.postcodes.io";

// Captive portal AP
const char* PORTAL_SSID = "PetrolStation-Setup";

// Station branding (the *device itself*, not the displayed fuel station)
const char* STATION_SUBTITLE = "fuel station";

// NTP
const char* NTP_SERVER = "time.google.com";
const char* TZ_UK = "GMT0BST,M3.5.0/1,M10.5.0/2";

// Timing
const uint32_t WIFI_CONNECT_TIMEOUT_MS = 15000;
const uint32_t FUEL_REFRESH_INTERVAL_MS = 60UL * 60UL * 1000UL; // 1 hour
const uint32_t SPINNER_FRAME_MS = 80;
const uint32_t DOTS_FRAME_MS = 400;

// Max stations we keep in memory after radius filter (well above typical count)
constexpr int MAX_STATIONS_IN_RADIUS = 80;

// Palette
#define COL_BG            0x10A2
#define COL_ACCENT        0xFB80
#define COL_LOGO          0x04FF
#define COL_NAME          0xFFE0
#define COL_SUBTITLE      0xFFFF
#define COL_DIVIDER       0x4208
#define COL_FOOTER_TEXT   0xC618
#define COL_WIFI_OK       0x07E0
#define COL_WIFI_BAD      0xF800
#define COL_SUCCESS       0x07E0
#define COL_ERROR         0xF800

// =============================================================
// 7-segment driver (daisy-chained, with spinner)
// =============================================================
constexpr uint8_t PIN_SEG_SCLK = 25;
constexpr uint8_t PIN_SEG_SDI  = 26;
constexpr uint8_t PIN_SEG_LOAD = 27;

static const uint8_t DIGIT_PATTERNS[] = {
  0b00111111, 0b00000110, 0b01011011, 0b01001111, 0b01100110,
  0b01101101, 0b01111101, 0b00000111, 0b01111111, 0b01101111,
};
constexpr uint8_t BLANK_PATTERN = 0b00000000;

static const uint8_t SPINNER_FRAMES[6] = {
  0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
};
constexpr bool REVERSE_DIGIT_SHIFT_ORDER = false;

enum class SegMode { PRICES, SPINNER, BLANK };

class FuelDisplayChain {
public:
  FuelDisplayChain() : _unleaded(0), _diesel(0),
                       _unleadedValid(false), _dieselValid(false),
                       _mode(SegMode::BLANK), _spinFrame(0), _lastFrameMs(0) {
    pinMode(PIN_SEG_SCLK, OUTPUT);
    pinMode(PIN_SEG_SDI,  OUTPUT);
    pinMode(PIN_SEG_LOAD, OUTPUT);
    digitalWrite(PIN_SEG_SCLK, LOW);
    digitalWrite(PIN_SEG_SDI,  LOW);
    digitalWrite(PIN_SEG_LOAD, LOW);
  }

  void setPrices(int unleaded, int diesel) {
    _unleaded = unleaded;
    _diesel = diesel;
    _unleadedValid = (unleaded >= 0 && unleaded <= 999);
    _dieselValid   = (diesel   >= 0 && diesel   <= 999);
    if (_mode == SegMode::PRICES) render();
  }

  void setMode(SegMode mode) {
    if (_mode == mode) return;
    _mode = mode;
    _spinFrame = 0;
    _lastFrameMs = millis();
    render();
  }

  void tick() {
    if (_mode == SegMode::SPINNER) {
      uint32_t now = millis();
      if (now - _lastFrameMs >= SPINNER_FRAME_MS) {
        _lastFrameMs = now;
        _spinFrame = (_spinFrame + 1) % 6;
        render();
      }
    }
  }

private:
  int _unleaded, _diesel;
  bool _unleadedValid, _dieselValid;
  SegMode _mode;
  uint8_t _spinFrame;
  uint32_t _lastFrameMs;

  uint8_t encodeDigit(uint8_t d) { return (d < 10) ? DIGIT_PATTERNS[d] : BLANK_PATTERN; }

  void buildModulePatterns(int value, bool valid, uint8_t out[3]) {
    if (!valid) { out[0]=out[1]=out[2]=BLANK_PATTERN; return; }
    uint8_t d2 = value % 10;
    uint8_t d1 = (value / 10) % 10;
    uint8_t d0 = (value / 100) % 10;
    out[0] = (d0 == 0) ? BLANK_PATTERN : encodeDigit(d0);
    out[1] = (d0 == 0 && d1 == 0) ? BLANK_PATTERN : encodeDigit(d1);
    out[2] = encodeDigit(d2);
  }

  void render() {
    uint8_t patterns[6];
    switch (_mode) {
      case SegMode::PRICES: {
        uint8_t u[3], d[3];
        buildModulePatterns(_unleaded, _unleadedValid, u);
        buildModulePatterns(_diesel, _dieselValid, d);
        for (int i=0; i<3; i++) { patterns[i]=u[i]; patterns[3+i]=d[i]; }
        break;
      }
      case SegMode::SPINNER: {
        uint8_t f = SPINNER_FRAMES[_spinFrame];
        for (int i=0; i<6; i++) patterns[i] = f;
        break;
      }
      default:
        for (int i=0; i<6; i++) patterns[i] = BLANK_PATTERN;
        break;
    }
    uint8_t out[6];
    for (int i=0; i<6; i++) out[i] = ~patterns[i];

    digitalWrite(PIN_SEG_LOAD, LOW);
    if (REVERSE_DIGIT_SHIFT_ORDER) {
      for (int i=0; i<6; i++) shiftOut(PIN_SEG_SDI, PIN_SEG_SCLK, MSBFIRST, out[i]);
    } else {
      for (int i=5; i>=0; i--) shiftOut(PIN_SEG_SDI, PIN_SEG_SCLK, MSBFIRST, out[i]);
    }
    digitalWrite(PIN_SEG_LOAD, HIGH);
    digitalWrite(PIN_SEG_LOAD, LOW);
  }
};

FuelDisplayChain fuel;

// =============================================================
// Display
// =============================================================
TFT_eSPI tft = TFT_eSPI();
constexpr int SCREEN_W = 320, SCREEN_H = 240;
constexpr int FOOTER_H = 28;
constexpr int FOOTER_Y = SCREEN_H - FOOTER_H;
constexpr int DIVIDER_Y = FOOTER_Y - 4;

int signalBars(int32_t rssi) {
  if (rssi >= -55) return 4;
  if (rssi >= -65) return 3;
  if (rssi >= -75) return 2;
  if (rssi >= -85) return 1;
  return 0;
}

void drawDroplet(int cx, int cy, int h, uint16_t col) {
  int r = h / 3;
  int circleCy = cy + h / 4;
  tft.fillCircle(cx, circleCy, r, col);
  int topY = cy - h / 2;
  int baseY = circleCy - r + 2;
  tft.fillTriangle(cx, topY, cx - r, baseY, cx + r, baseY, col);
  tft.fillCircle(cx - r / 3, circleCy - r / 3, r / 6, COL_SUBTITLE);
}

void drawWifiIcon(int x, int y, int bars, uint16_t col) {
  const int barW = 3, barGap = 2;
  const int heights[4] = {4, 7, 10, 13};
  for (int i=0; i<4; i++) {
    int bx = x + i * (barW + barGap), bh = heights[i], by = y - bh;
    uint16_t c = (i < bars) ? col : COL_DIVIDER;
    tft.fillRect(bx, by, barW, bh, c);
  }
}

// Current "station shown on screen" — empty on first boot
String displayedStationName = "";
String displayedStationTown = "";

void drawHero() {
  tft.fillRect(0, 0, SCREEN_W, DIVIDER_Y, COL_BG);
  int cx = SCREEN_W / 2;
  drawDroplet(cx, 55, 70, COL_LOGO);
  tft.setTextDatum(MC_DATUM);

  if (displayedStationName.length() == 0) {
    // Pre-fetch state
    tft.setTextColor(COL_NAME, COL_BG);
    tft.drawString("Finding cheapest fuel...", cx, 140, 2);
  } else {
    tft.setTextColor(COL_NAME, COL_BG);
    // Truncate long names to fit
    String name = displayedStationName;
    if (tft.textWidth(name.c_str(), 4) > SCREEN_W - 20) {
      while (tft.textWidth((name + "...").c_str(), 4) > SCREEN_W - 20 && name.length() > 4) {
        name.remove(name.length() - 1);
      }
      name += "...";
    }
    tft.drawString(name.c_str(), cx, 135, 4);

    tft.setTextColor(COL_SUBTITLE, COL_BG);
    tft.drawString(displayedStationTown.c_str(), cx, 170, 2);
  }

  tft.drawFastHLine(20, DIVIDER_Y, SCREEN_W - 40, COL_DIVIDER);
}

void drawFooter(const char* timeStr, int wifiBars, const char* wifiLabel, uint16_t wifiCol) {
  tft.fillRect(0, FOOTER_Y, SCREEN_W, FOOTER_H, COL_BG);
  tft.setTextDatum(ML_DATUM);
  tft.setTextColor(COL_FOOTER_TEXT, COL_BG);
  tft.drawString(timeStr, 12, FOOTER_Y + FOOTER_H / 2, 2);

  int iconY = FOOTER_Y + FOOTER_H - 6;
  int labelRightX = SCREEN_W - 12;
  tft.setTextDatum(MR_DATUM);
  tft.setTextColor(wifiCol, COL_BG);
  int labelW = tft.textWidth(wifiLabel, 2);
  tft.drawString(wifiLabel, labelRightX, FOOTER_Y + FOOTER_H / 2, 2);
  int iconX = labelRightX - labelW - 28;
  uint16_t iconCol = (wifiBars > 0) ? COL_WIFI_OK : COL_WIFI_BAD;
  drawWifiIcon(iconX, iconY, wifiBars, iconCol);
}

void drawCenteredStatus(const char* title, uint16_t titleCol,
                        const char* line1, const char* line2) {
  tft.fillRect(0, 0, SCREEN_W, DIVIDER_Y, COL_BG);
  int cx = SCREEN_W / 2;
  drawDroplet(cx, 45, 55, COL_LOGO);
  tft.setTextDatum(MC_DATUM);
  tft.setTextColor(titleCol, COL_BG);
  tft.drawString(title, cx, 110, 4);
  if (line1) { tft.setTextColor(COL_SUBTITLE, COL_BG); tft.drawString(line1, cx, 150, 2); }
  if (line2) { tft.setTextColor(COL_FOOTER_TEXT, COL_BG); tft.drawString(line2, cx, 175, 2); }
}

// =============================================================
// Preferences (NVS storage)
// =============================================================
Preferences prefs;

String savedSSID()      { return prefs.getString("ssid", ""); }
String savedPass()      { return prefs.getString("pass", ""); }
String savedPostcode()  { return prefs.getString("postcode", ""); }
int    savedRadius()    { return prefs.getInt("radius_mi", 5); }

int    cachedUnleaded() { return prefs.getInt("c_unl", -1); }
int    cachedDiesel()   { return prefs.getInt("c_dsl", -1); }
String cachedName()     { return prefs.getString("c_name", ""); }
String cachedTown()     { return prefs.getString("c_town", ""); }

void savePrices(int u, int d, const String& name, const String& town) {
  prefs.putInt("c_unl", u);
  prefs.putInt("c_dsl", d);
  prefs.putString("c_name", name);
  prefs.putString("c_town", town);
}

// =============================================================
// Captive Portal
// =============================================================
WebServer server(80);
DNSServer dnsServer;
bool portalActive = false;

String htmlEscape(const String& s) {
  String out;
  for (char c : s) {
    if (c == '<') out += "&lt;";
    else if (c == '>') out += "&gt;";
    else if (c == '&') out += "&amp;";
    else if (c == '"') out += "&quot;";
    else out += c;
  }
  return out;
}

String buildConfigPage(const String& message = "") {
  int n = WiFi.scanNetworks();
  String savedPc = savedPostcode();
  int savedR = savedRadius();

  String html = F("<!DOCTYPE html><html><head><meta name=viewport content='width=device-width,initial-scale=1'>"
                  "<title>Fuel Display Setup</title>"
                  "<style>"
                  "body{font-family:-apple-system,system-ui,sans-serif;background:#101820;color:#fff;margin:0;padding:20px;}"
                  "h1{color:#fb8020;margin-top:0;}"
                  "form{background:#1a2430;padding:20px;border-radius:8px;max-width:440px;}"
                  "label{display:block;margin-top:14px;color:#ccc;font-size:14px;}"
                  "input,select{width:100%;padding:10px;margin-top:4px;border-radius:4px;border:1px solid #444;background:#111;color:#fff;box-sizing:border-box;font-size:16px;}"
                  "button{margin-top:20px;padding:12px 20px;background:#fb8020;color:#101820;border:0;border-radius:4px;font-weight:bold;cursor:pointer;width:100%;font-size:16px;}"
                  ".msg{background:#442200;padding:10px;border-radius:4px;margin-bottom:10px;}"
                  ".hint{font-size:12px;color:#888;margin-top:4px;}"
                  "</style></head><body>"
                  "<h1>Fuel Display Setup</h1>");
  if (message.length()) {
    html += "<div class=msg>" + htmlEscape(message) + "</div>";
  }
  html += F("<form method=POST action=/save>"
            "<label>Wi-Fi network</label>"
            "<select name=ssid>");
  for (int i=0; i<n; i++) {
    String ssid = WiFi.SSID(i);
    int32_t rssi = WiFi.RSSI(i);
    html += "<option value='" + htmlEscape(ssid) + "'>" + htmlEscape(ssid) + " (" + String(rssi) + " dBm)</option>";
  }
  html += F("</select>"
            "<label>Wi-Fi password</label>"
            "<input type=password name=pass>"
            "<label>UK postcode</label>"
            "<input type=text name=postcode placeholder='e.g. NR31 0DF' value='");
  html += htmlEscape(savedPc);
  html += F("' required>"
            "<div class=hint>Used to find cheapest fuel nearby.</div>"
            "<label>Search radius (miles)</label>"
            "<input type=number name=radius min=1 max=50 value='");
  html += String(savedR);
  html += F("' required>"
            "<button type=submit>Save and connect</button>"
            "</form></body></html>");
  return html;
}

void handleRoot()    { server.send(200, "text/html", buildConfigPage()); }
void handleCaptive() { server.sendHeader("Location", "/", true); server.send(302, "text/plain", ""); }

void handleSave() {
  String ssid = server.arg("ssid");
  String pass = server.arg("pass");
  String pc   = server.arg("postcode");
  int radius  = server.arg("radius").toInt();
  pc.trim(); pc.toUpperCase();

  if (ssid.length() == 0 || pc.length() == 0 || radius < 1) {
    server.send(400, "text/html", buildConfigPage("All fields required."));
    return;
  }

  prefs.putString("ssid", ssid);
  prefs.putString("pass", pass);
  prefs.putString("postcode", pc);
  prefs.putInt("radius_mi", radius);

  String body = F("<!DOCTYPE html><html><body style='font-family:sans-serif;background:#101820;color:#fff;padding:20px;'>"
                  "<h2>Saved. Restarting...</h2></body></html>");
  server.send(200, "text/html", body);
  delay(1000);
  ESP.restart();
}

void startCaptivePortal() {
  Serial.println("[portal] starting");
  portalActive = true;
  fuel.setMode(SegMode::BLANK);

  WiFi.mode(WIFI_AP);
  WiFi.softAP(PORTAL_SSID);
  IPAddress ip = WiFi.softAPIP();
  Serial.printf("[portal] AP IP: %s\n", ip.toString().c_str());

  dnsServer.start(53, "*", ip);
  server.on("/", HTTP_GET, handleRoot);
  server.on("/save", HTTP_POST, handleSave);
  server.onNotFound(handleCaptive);
  server.begin();

  char ipStr[32];
  snprintf(ipStr, sizeof(ipStr), "If no page: %s", ip.toString().c_str());
  tft.fillRect(0, 0, SCREEN_W, DIVIDER_Y, COL_BG);
  int cx = SCREEN_W/2;
  drawDroplet(cx, 40, 50, COL_LOGO);
  tft.setTextDatum(MC_DATUM);
  tft.setTextColor(COL_NAME, COL_BG);
  tft.drawString("Setup Wi-Fi", cx, 88, 4);
  tft.setTextColor(COL_SUBTITLE, COL_BG);
  tft.drawString("Connect phone to:", cx, 120, 2);
  tft.setTextColor(COL_ACCENT, COL_BG);
  tft.drawString(PORTAL_SSID, cx, 145, 4);
  tft.setTextColor(COL_FOOTER_TEXT, COL_BG);
  tft.drawString("A setup page should open.", cx, 180, 2);
  tft.drawString(ipStr, cx, 198, 2);
}

// =============================================================
// Wi-Fi connection
// =============================================================
bool tryConnectStored() {
  String ssid = savedSSID();
  String pass = savedPass();
  if (ssid.length() == 0) return false;

  Serial.printf("[wifi] connecting to %s\n", ssid.c_str());
  fuel.setMode(SegMode::SPINNER);

  drawCenteredStatus("Connecting", COL_NAME, (String("to ") + ssid).c_str(), nullptr);

  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid.c_str(), pass.c_str());

  uint32_t start = millis();
  uint32_t lastDots = 0;
  int dotCount = 0;

  while (millis() - start < WIFI_CONNECT_TIMEOUT_MS) {
    fuel.tick();
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("[wifi] connected");
      configTzTime(TZ_UK, NTP_SERVER);
      return true;
    }
    uint32_t now = millis();
    if (now - lastDots >= DOTS_FRAME_MS) {
      lastDots = now;
      dotCount = (dotCount + 1) % 7;
      tft.fillRect(0, 160, SCREEN_W, 40, COL_BG);
      char dots[8] = "      ";
      for (int i=0; i<dotCount && i<6; i++) dots[i] = '.';
      tft.setTextDatum(MC_DATUM);
      tft.setTextColor(COL_ACCENT, COL_BG);
      tft.drawString(dots, SCREEN_W/2, 175, 4);
    }
    delay(10);
  }
  Serial.println("[wifi] timeout");
  return false;
}

// =============================================================
// HTTP helpers
// =============================================================

// Non-blocking delay that keeps spinner animation running.
void tickingDelay(uint32_t ms) {
  uint32_t end = millis() + ms;
  while (millis() < end) {
    fuel.tick();
    delay(5);
  }
}

// POST JSON body with retry. Heap-allocated client for safer cleanup.
bool httpPostJson(const String& url, const String& body, String& response) {
  WiFiClientSecure* client = new WiFiClientSecure();
  if (!client) { Serial.println("[http] alloc client failed"); return false; }

  client->setInsecure();
  client->setHandshakeTimeout(30);
  client->setTimeout(30);

  bool ok = false;
  {
    HTTPClient http;
    if (http.begin(*client, url)) {
      http.setTimeout(30000);
      http.setConnectTimeout(15000);
      http.addHeader("Content-Type", "application/json");
      http.addHeader("Accept", "application/json");
      http.addHeader("User-Agent", "ESP32-FuelDisplay/1.0");
      int code = http.POST(body);
      Serial.printf("[http] POST %s -> %d\n", url.c_str(), code);
      response = http.getString();
      ok = (code > 0 && code < 300);
      http.end();
    }
  }

  delete client;
  return ok;
}

// GET with optional bearer auth. Returns full body on success.
bool httpGetString(const String& url, const String& bearer, String& response) {
  WiFiClientSecure* client = new WiFiClientSecure();
  if (!client) { Serial.println("[http] alloc client failed"); return false; }

  client->setInsecure();
  client->setHandshakeTimeout(30);
  client->setTimeout(30);

  bool ok = false;
  {
    HTTPClient http;
    if (http.begin(*client, url)) {
      http.setTimeout(30000);
      http.setConnectTimeout(15000);
      http.addHeader("Accept", "application/json");
      http.addHeader("User-Agent", "ESP32-FuelDisplay/1.0");
      if (bearer.length()) http.addHeader("Authorization", "Bearer " + bearer);

      int code = http.GET();
      int contentLen = http.getSize();
      Serial.printf("[http] GET %s -> %d (content-length: %d)\n",
                    url.c_str(), code, contentLen);

      response = http.getString();

      // If getString returned empty but there's supposed to be a body, try stream read
      if (code > 0 && code < 300 && response.length() == 0 && contentLen != 0) {
        Serial.println("[http] getString empty, trying stream read...");
        WiFiClient* stream = http.getStreamPtr();
        uint32_t start = millis();
        while (stream->connected() && (millis() - start) < 20000) {
          while (stream->available()) {
            response += (char)stream->read();
          }
          if (contentLen > 0 && (int)response.length() >= contentLen) break;
          delay(10);
          fuel.tick();
        }
        Serial.printf("[http] stream read got %d bytes\n", response.length());
      }

      if (code > 0 && code < 300) {
        ok = true;
      } else {
        if (response.length()) {
          Serial.println("[http] error body:");
          Serial.println(response);
        }
      }
      http.end();
    }
  }

  delete client;
  return ok;
}

// =============================================================
// Geocoding (postcodes.io)
// =============================================================
GeoLocation geocodePostcode(const String& postcode) {
  GeoLocation g = { 0, 0, "", false };
  String url = String(POSTCODES_IO_BASE) + "/postcodes/" + postcode;
  url.replace(" ", "");

  String response;
  if (!httpGetString(url, "", response)) return g;

  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, response);
  if (err) { Serial.printf("[geo] parse err: %s\n", err.c_str()); return g; }

  if (doc["status"].as<int>() == 200) {
    g.lat = doc["result"]["latitude"].as<double>();
    g.lon = doc["result"]["longitude"].as<double>();
    const char* t = doc["result"]["admin_district"] | doc["result"]["parish"] | doc["result"]["region"] | "";
    g.town = String(t);
    g.valid = true;
  }
  return g;
}

// =============================================================
// Fuel Finder OAuth + fetch
// =============================================================
String fuelFinderToken;

bool fetchAccessToken() {
  String url = String(FUEL_FINDER_BASE) + "/api/v1/oauth/generate_access_token";
  JsonDocument reqDoc;
  reqDoc["grant_type"] = "client_credentials";
  reqDoc["client_id"] = FUEL_FINDER_CLIENT_ID;
  reqDoc["client_secret"] = FUEL_FINDER_CLIENT_SECRET;
  String body;
  serializeJson(reqDoc, body);

  // Safety: refuse to proceed if credentials weren't replaced
  if (String(FUEL_FINDER_CLIENT_ID).indexOf("YOUR_") >= 0 ||
      String(FUEL_FINDER_CLIENT_SECRET).indexOf("YOUR_") >= 0) {
    Serial.println("[oauth] credentials not set in sketch");
    return false;
  }

  // Retry up to 3 times — first TLS handshake to a new host sometimes stalls
  String response;
  bool ok = false;
  for (int attempt = 1; attempt <= 3 && !ok; attempt++) {
    Serial.printf("[oauth] attempt %d\n", attempt);
    ok = httpPostJson(url, body, response);
    if (!ok && attempt < 3) {
      Serial.println("[oauth] retrying in 2s...");
      tickingDelay(2000);
    }
  }
  if (!ok) {
    Serial.println("[oauth] all retries failed");
    return false;
  }

  Serial.println("[oauth] raw response:");
  Serial.println(response);

  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, response);
  if (err) { Serial.printf("[oauth] parse err: %s\n", err.c_str()); return false; }

  const char* tok = doc["access_token"] | doc["accessToken"] | doc["token"] | (const char*)nullptr;
  if (!tok) {
    tok = doc["data"]["access_token"] | doc["data"]["accessToken"] | (const char*)nullptr;
  }
  if (!tok) { Serial.println("[oauth] no access_token in response"); return false; }
  fuelFinderToken = String(tok);
  Serial.printf("[oauth] token acquired (length %d)\n", fuelFinderToken.length());
  return true;
}

// Haversine distance in miles
double haversineMiles(double lat1, double lon1, double lat2, double lon2) {
  const double R = 3958.8; // Earth radius in miles
  double dLat = (lat2 - lat1) * M_PI / 180.0;
  double dLon = (lon2 - lon1) * M_PI / 180.0;
  double a = sin(dLat/2)*sin(dLat/2) +
             cos(lat1*M_PI/180.0) * cos(lat2*M_PI/180.0) *
             sin(dLon/2)*sin(dLon/2);
  return 2 * R * atan2(sqrt(a), sqrt(1-a));
}

// Stations within radius (kept in memory). Keyed by site_id for price lookup.
NearbyStation nearby[MAX_STATIONS_IN_RADIUS];
int nearbyCount = 0;

// Stream-parse from HTTP response so we never hold the full body in RAM.
// ArduinoJson with filter reads bytes sequentially and keeps only matched fields.
bool fetchStationsWithinRadius(double originLat, double originLon, double radiusMi) {
  nearbyCount = 0;
  int batch = 1;

  while (true) {
    String url = String(FUEL_FINDER_BASE) + "/api/v1/pfs?batch-number=" + String(batch);
    WiFiClientSecure* client = new WiFiClientSecure();
    if (!client) { Serial.println("[pfs] client alloc failed"); return false; }
    client->setInsecure();
    client->setHandshakeTimeout(30);
    client->setTimeout(30);

    HTTPClient http;
    bool iterOk = false;
    int pageItemCount = 0;

    if (http.begin(*client, url)) {
      http.setTimeout(60000);
      http.setConnectTimeout(15000);
      http.addHeader("Accept", "application/json");
      http.addHeader("User-Agent", "ESP32-FuelDisplay/1.0");
      http.addHeader("Authorization", "Bearer " + fuelFinderToken);

      int code = http.GET();
      int contentLen = http.getSize();
      Serial.printf("[pfs] batch %d GET -> %d (len %d)\n", batch, code, contentLen);

      if (code > 0 && code < 300) {
        // Feed a watchdog-aware Stream wrapper into ArduinoJson.
        // The filter tells the parser to only keep these fields — everything else is discarded in-flight.
        JsonDocument filter;
        filter[0]["node_id"] = true;
        filter[0]["trading_name"] = true;
        filter[0]["brand_name"] = true;
        filter[0]["permanent_closure"] = true;
        filter[0]["temporary_closure"] = true;
        filter[0]["location"] = true;

        JsonDocument doc;
        Serial.println("[pfs] streaming parse begins...");
        uint32_t parseStart = millis();

        DeserializationError err = deserializeJson(doc,
                                                   *http.getStreamPtr(),
                                                   DeserializationOption::Filter(filter));
        uint32_t parseTime = millis() - parseStart;
        Serial.printf("[pfs] parse took %lu ms, result: %s\n", parseTime, err.c_str());

        if (err) {
          // IncompleteInput may just mean the connection is still streaming — try again with more read time.
          Serial.printf("[pfs] parse err batch %d: %s\n", batch, err.c_str());
        } else {
          JsonArray items = doc.as<JsonArray>();
          for (JsonObject item : items) {
            pageItemCount++;
            if ((pageItemCount & 0x1F) == 0) fuel.tick();
            if (nearbyCount >= MAX_STATIONS_IN_RADIUS) break;

            bool perm = item["permanent_closure"] | false;
            bool temp = item["temporary_closure"] | false;
            if (perm || temp) continue;

            JsonVariant loc = item["location"];
            if (batch == 1 && pageItemCount <= 2) {
              Serial.printf("[pfs] station %d location: ", pageItemCount);
              serializeJson(loc, Serial);
              Serial.println();
            }

            double lat = 0.0, lon = 0.0;
            if (loc.is<JsonObject>()) {
              lat = loc["latitude"]  | loc["lat"] | loc["Latitude"] | 0.0;
              lon = loc["longitude"] | loc["lon"] | loc["lng"] | loc["Longitude"] | 0.0;
            }
            if (lat == 0.0 && lon == 0.0) continue;

            double dist = haversineMiles(originLat, originLon, lat, lon);
            if (dist > radiusMi) continue;

            NearbyStation& s = nearby[nearbyCount++];
            s.siteId = item["node_id"].as<const char*>() ? item["node_id"].as<const char*>() : "";
            s.brand  = item["brand_name"].as<const char*>() ? item["brand_name"].as<const char*>() : "";
            s.name   = item["trading_name"].as<const char*>() ? item["trading_name"].as<const char*>() : "";
            s.town   = "";
            s.lat = lat; s.lon = lon; s.distanceMi = dist;
            s.priceE10_ppl = -1; s.priceB7_ppl = -1;
          }
          iterOk = true;
          Serial.printf("[pfs] batch %d: %d stations, %d in radius total\n",
                        batch, pageItemCount, nearbyCount);
        }
      } else {
        Serial.printf("[pfs] HTTP error %d\n", code);
      }
      http.end();
    }
    delete client;

    if (!iterOk) return false;
    if (pageItemCount < 400) break;
    batch++;
    tickingDelay(2100);
  }
  Serial.printf("[pfs] %d stations within %.1f mi\n", nearbyCount, radiusMi);
  return true;
}

// Stream-parse fuel prices.
bool fetchPrices() {
  int batch = 1;
  while (true) {
    String url = String(FUEL_FINDER_BASE) + "/api/v1/pfs/fuel-prices?batch-number=" + String(batch);
    WiFiClientSecure* client = new WiFiClientSecure();
    if (!client) { Serial.println("[prices] client alloc failed"); return false; }
    client->setInsecure();
    client->setHandshakeTimeout(30);
    client->setTimeout(30);

    HTTPClient http;
    bool iterOk = false;
    int pageItemCount = 0;

    if (http.begin(*client, url)) {
      http.setTimeout(60000);
      http.setConnectTimeout(15000);
      http.addHeader("Accept", "application/json");
      http.addHeader("User-Agent", "ESP32-FuelDisplay/1.0");
      http.addHeader("Authorization", "Bearer " + fuelFinderToken);

      int code = http.GET();
      int contentLen = http.getSize();
      Serial.printf("[prices] batch %d GET -> %d (len %d)\n", batch, code, contentLen);

      if (code > 0 && code < 300) {
        JsonDocument filter;
        filter[0]["node_id"] = true;
        filter[0]["fuel_prices"] = true;

        JsonDocument doc;
        Serial.println("[prices] streaming parse begins...");
        uint32_t parseStart = millis();
        DeserializationError err = deserializeJson(doc,
                                                   *http.getStreamPtr(),
                                                   DeserializationOption::Filter(filter));
        uint32_t parseTime = millis() - parseStart;
        Serial.printf("[prices] parse took %lu ms, result: %s\n", parseTime, err.c_str());

        if (!err) {
          JsonArray items = doc.as<JsonArray>();
          int matched = 0;
          for (JsonObject item : items) {
            pageItemCount++;
            if ((pageItemCount & 0x1F) == 0) fuel.tick();
            const char* nodeId = item["node_id"] | "";
            JsonVariant fuelPrices = item["fuel_prices"];

            if (batch == 1 && pageItemCount <= 2) {
              Serial.printf("[prices] station %d fuel_prices: ", pageItemCount);
              serializeJson(fuelPrices, Serial);
              Serial.println();
            }

            int idx = -1;
            for (int i=0; i<nearbyCount; i++) {
              if (nearby[i].siteId == nodeId) { idx = i; break; }
            }
            if (idx < 0 || !fuelPrices.is<JsonArray>()) continue;

            for (JsonObject fp : fuelPrices.as<JsonArray>()) {
              const char* type = fp["fuel_type"] | fp["grade"] | fp["fuel_grade"]
                               | fp["type"] | fp["name"] | "";
              double priceVal = fp["price"] | fp["price_per_litre"] | fp["pricePerLitre"]
                              | fp["ppl"] | fp["amount"] | 0.0;
              if (priceVal <= 0.0 || type[0] == 0) continue;
              int ppl = (priceVal > 10.0) ? (int)round(priceVal) : (int)round(priceVal * 100.0);
              if (strstr(type, "E10") || strcasecmp(type, "unleaded") == 0) {
                nearby[idx].priceE10_ppl = ppl; matched++;
              } else if (strstr(type, "B7") || strcasecmp(type, "diesel") == 0) {
                nearby[idx].priceB7_ppl = ppl; matched++;
              }
            }
          }
          iterOk = true;
          Serial.printf("[prices] batch %d: %d stations, %d matches\n",
                        batch, pageItemCount, matched);
        }
      }
      http.end();
    }
    delete client;

    if (!iterOk) return false;
    if (pageItemCount < 400) break;
    batch++;
    tickingDelay(2100);
  }
  return true;
}

// Main refresh cycle.
// @param silent true = no spinner, no status screens (for background hourly refreshes)
bool refreshFuelData(bool silent = false) {
  // Disable task WDT during refresh — long HTTPS reads will starve it otherwise
  esp_task_wdt_deinit();

  Serial.printf("[fuel] refresh start (silent=%d, free heap: %d, largest block: %d)\n",
                silent, ESP.getFreeHeap(), ESP.getMaxAllocHeap());

  // Heap safety gate: TLS needs ~50KB contiguous. Abort if we're dangerously low.
  if (ESP.getMaxAllocHeap() < 60000) {
    Serial.println("[fuel] heap too fragmented, rebooting for clean state");
    delay(1000);
    ESP.restart();
  }

  if (!silent) {
    fuel.setMode(SegMode::SPINNER);
    drawCenteredStatus("Fetching prices...", COL_NAME, "Geocoding postcode", nullptr);
  }

  // Brief settle delay lets WiFi stack finish any background work
  tickingDelay(500);

  GeoLocation geo = geocodePostcode(savedPostcode());
  if (!geo.valid) {
    Serial.println("[fuel] geocode failed");
    if (!silent) {
      drawCenteredStatus("Postcode error", COL_ERROR, "Check postcode in setup", nullptr);
      delay(3000);
    }
    return false;
  }
  Serial.printf("[fuel] origin %.4f,%.4f (%s)\n", geo.lat, geo.lon, geo.town.c_str());
  Serial.printf("[fuel] heap after geocode: %d / largest %d\n",
                ESP.getFreeHeap(), ESP.getMaxAllocHeap());

  if (!silent) drawCenteredStatus("Fetching prices...", COL_NAME, "Authenticating", nullptr);
  if (!fetchAccessToken()) {
    if (!silent) {
      drawCenteredStatus("Auth failed", COL_ERROR, "Check API credentials", nullptr);
      delay(3000);
    }
    return false;
  }

  if (!silent) drawCenteredStatus("Fetching prices...", COL_NAME, "Finding nearby stations", nullptr);
  if (!fetchStationsWithinRadius(geo.lat, geo.lon, savedRadius())) {
    if (!silent) {
      drawCenteredStatus("Fetch failed", COL_ERROR, "Stations list", nullptr);
      delay(3000);
    }
    return false;
  }
  if (nearbyCount == 0) {
    if (!silent) {
      drawCenteredStatus("No stations found", COL_ERROR, "Try a larger radius", nullptr);
      delay(3000);
    }
    return false;
  }

  if (!silent) drawCenteredStatus("Fetching prices...", COL_NAME, "Loading price data", nullptr);
  if (!fetchPrices()) {
    if (!silent) {
      drawCenteredStatus("Fetch failed", COL_ERROR, "Price data", nullptr);
      delay(3000);
    }
    return false;
  }

  // Pick cheapest E10 station
  int bestIdx = -1;
  int bestE10 = INT_MAX;
  for (int i=0; i<nearbyCount; i++) {
    if (nearby[i].priceE10_ppl > 0 && nearby[i].priceE10_ppl < bestE10) {
      bestE10 = nearby[i].priceE10_ppl;
      bestIdx = i;
    }
  }
  if (bestIdx < 0) {
    if (!silent) {
      drawCenteredStatus("No unleaded data", COL_ERROR, "None of the nearby stations", "had E10 prices");
      delay(3000);
    }
    return false;
  }

  NearbyStation& best = nearby[bestIdx];
  Serial.printf("[fuel] cheapest E10: %s (%s) %dppl E10, %dppl B7, %.2fmi\n",
                best.name.c_str(), best.brand.c_str(),
                best.priceE10_ppl, best.priceB7_ppl, best.distanceMi);

  // Update UI
  String shownName = best.brand.length() ? best.brand : best.name;
  displayedStationName = shownName;
  displayedStationTown = best.town.length() ? best.town : geo.town;
  fuel.setPrices(best.priceE10_ppl, best.priceB7_ppl > 0 ? best.priceB7_ppl : 0);
  fuel.setMode(SegMode::PRICES);
  savePrices(best.priceE10_ppl, best.priceB7_ppl, displayedStationName, displayedStationTown);

  drawHero();
  Serial.println("[fuel] refresh complete");
  return true;
}

// =============================================================
// Setup & Loop
// =============================================================
bool fetchTimeString(char* out, size_t outSize) {
  struct tm tm;
  if (!getLocalTime(&tm, 100)) return false;
  strftime(out, outSize, "%l:%M %p", &tm);
  if (out[0] == ' ') memmove(out, out + 1, strlen(out));
  return true;
}

void setup() {
  Serial.begin(115200);
  delay(200);
  Serial.println("[boot] petrol station display");

  tft.init();
  tft.setRotation(1);
  prefs.begin("fuelstation", false);

  // Restore cached display so screen isn't empty during first refresh
  displayedStationName = cachedName();
  displayedStationTown = cachedTown();
  int cu = cachedUnleaded(), cd = cachedDiesel();
  if (cu > 0) fuel.setPrices(cu, cd > 0 ? cd : 0);
  fuel.setMode(SegMode::BLANK);

  drawHero();
  drawFooter("--:--", 0, "starting", COL_FOOTER_TEXT);

  // Must have Wi-Fi creds AND a postcode to go live
  if (savedSSID().length() == 0 || savedPostcode().length() == 0) {
    startCaptivePortal();
    return;
  }

  if (!tryConnectStored()) {
    startCaptivePortal();
    return;
  }

  // Connected — do first refresh (visible so user sees progress)
  refreshFuelData(false);
}

uint32_t lastRefreshMs = 0;
bool lastRefreshOk = false;
int consecutiveFailures = 0;
const uint32_t RETRY_AFTER_FAIL_MS = 5UL * 60UL * 1000UL;  // 5 min after a failure
const int MAX_CONSECUTIVE_FAILURES = 3;  // reboot after this many

void loop() {
  fuel.tick();

  if (portalActive) {
    dnsServer.processNextRequest();
    server.handleClient();
    delay(2);
    return;
  }

  uint32_t interval = lastRefreshOk ? FUEL_REFRESH_INTERVAL_MS : RETRY_AFTER_FAIL_MS;
  if (millis() - lastRefreshMs >= interval || lastRefreshMs == 0) {
    if (WiFi.status() == WL_CONNECTED) {
      lastRefreshMs = millis();
      // First refresh of this boot = visible; subsequent = silent
      bool silent = lastRefreshOk;
      lastRefreshOk = refreshFuelData(silent);
      if (lastRefreshOk) {
        consecutiveFailures = 0;
      } else {
        consecutiveFailures++;
        Serial.printf("[fuel] consecutive failures: %d\n", consecutiveFailures);
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          Serial.println("[fuel] too many failures, rebooting");
          delay(1000);
          ESP.restart();
        }
      }
    }
  }

  // 1Hz UI tick
  static uint32_t lastUiTick = 0;
  if (millis() - lastUiTick < 1000) { delay(20); return; }
  lastUiTick = millis();

  char timeStr[16] = "--:--";
  if (WiFi.status() == WL_CONNECTED) fetchTimeString(timeStr, sizeof(timeStr));

  if (WiFi.status() == WL_CONNECTED) {
    int32_t rssi = WiFi.RSSI();
    int bars = signalBars(rssi);
    char label[16];
    snprintf(label, sizeof(label), "%d dBm", rssi);
    drawFooter(timeStr, bars, label, COL_WIFI_OK);
  } else {
    drawFooter(timeStr, 0, "offline", COL_WIFI_BAD);
  }
}
