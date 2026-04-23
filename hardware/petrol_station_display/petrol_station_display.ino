/*
 * Petrol Station Display — ESP32-WROOM-32
 *
 * Live UK fuel price display:
 *   - Waveshare 2" ST7789V 320x240 landscape LCD shows station name/town/distance
 *   - Two 3-digit 7-seg modules (daisy-chained) show unleaded/diesel prices in ppl
 *
 * Data flow:
 *   ESP32 -> proxy (http://...:8080/cheapest) -> tiny JSON response
 *
 * The proxy (FastAPI on a UK VM) holds the OAuth credentials, refreshes the
 * full Fuel Finder dataset hourly, does the geocoding, haversine, and
 * cheapest-station selection server-side. The ESP32 does one HTTP GET per
 * hour and parses ~250 bytes.
 *
 * Required libraries:
 *   - TFT_eSPI (Bodmer) with custom User_Setup.h
 *   - ArduinoJson v7+ (Benoit Blanchon)
 *   - WiFi, WebServer, DNSServer, Preferences, HTTPClient (built-in)
 *
 * Board: ESP32 Dev Module
 */

#include <Arduino.h>
#include <SPI.h>
#include <TFT_eSPI.h>
#include <WiFi.h>
#include <HTTPClient.h>
#include <FS.h>
using fs::FS;  // ESP32 core v3.x needs this before <WebServer.h>
#include <WebServer.h>
#include <DNSServer.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <time.h>

#include "logos.h"

// =============================================================
// Types
// =============================================================
struct CheapestStation {
  String name;
  String brand;
  String brandSlug;      // canonical slug from proxy: "shell", "bp", "esso", ...
  String town;
  String postcode;
  String address;
  bool   isMotorway;
  bool   isSupermarket;
  int    priceE10_ppl;   // rounded to whole pence
  int    priceB7_ppl;
  double distanceMi;
  uint32_t updatedAt;    // unix seconds from proxy
};

// =============================================================
// CONFIG
// =============================================================
const char* PORTAL_SSID = "PetrolStation-Setup";
const char* PROXY_URL_DEFAULT = "http://144.21.57.147:8080";

const char* NTP_SERVER = "time.google.com";
const char* TZ_UK = "GMT0BST,M3.5.0/1,M10.5.0/2";

const uint32_t WIFI_CONNECT_TIMEOUT_MS = 15000;
const uint32_t REFRESH_INTERVAL_MS     = 60UL * 60UL * 1000UL; // 1 hour
const uint32_t RETRY_AFTER_FAIL_MS     = 5UL * 60UL * 1000UL;  // 5 min
const int      MAX_CONSECUTIVE_FAILURES = 5;
const uint32_t SPINNER_FRAME_MS = 80;
const uint32_t DOTS_FRAME_MS    = 400;

// Palette
#define COL_BG            0x10A2
#define COL_ACCENT        0xFB80
#define COL_LOGO          0x04FF
#define COL_NAME          0xFFE0
#define COL_SUBTITLE      0xFFFF
#define COL_DIVIDER       0x4208
#define COL_FOOTER_TEXT   0xC618
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

  // Hand-rolled shift with explicit edge delays. Arduino's shiftOut on the
  // ESP32 can toggle fast enough that long jumper wires ring into spurious
  // SCLK edges, jumbling bits in the chain. Adding small waits lets each
  // edge settle before the next one fires.
  static inline void shiftByte(uint8_t val) {
    for (int i = 7; i >= 0; i--) {
      digitalWrite(PIN_SEG_SDI, (val >> i) & 1);
      delayMicroseconds(3);
      digitalWrite(PIN_SEG_SCLK, HIGH);
      delayMicroseconds(3);
      digitalWrite(PIN_SEG_SCLK, LOW);
      delayMicroseconds(1);
    }
  }

  void buildModulePatterns(int value, bool valid, uint8_t out[3]) {
    if (!valid) { out[0]=out[1]=out[2]=BLANK_PATTERN; return; }
    uint8_t d2 = value % 10;
    uint8_t d1 = (value / 10) % 10;
    uint8_t d0 = (value / 100) % 10;
    out[0] = (d0 == 0) ? BLANK_PATTERN : encodeDigit(d0);
    out[1] = (d0 == 0 && d1 == 0) ? BLANK_PATTERN : encodeDigit(d1);
    out[2] = encodeDigit(d2);
  }

  static inline void shiftAndLatch(const uint8_t out[6]) {
    digitalWrite(PIN_SEG_LOAD, LOW);
    if (REVERSE_DIGIT_SHIFT_ORDER) {
      for (int i=0; i<6; i++) shiftByte(out[i]);
    } else {
      for (int i=5; i>=0; i--) shiftByte(out[i]);
    }
    delayMicroseconds(2);
    digitalWrite(PIN_SEG_LOAD, HIGH);
    delayMicroseconds(5);
    digitalWrite(PIN_SEG_LOAD, LOW);
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

    // Shift twice with a brief gap. If the first pass dropped or gained a
    // clock edge (ringing / WiFi RF), the second reloads the chain with the
    // correct state before the user notices.
    shiftAndLatch(out);
    delayMicroseconds(200);
    shiftAndLatch(out);
  }
};

FuelDisplayChain fuel;

// =============================================================
// TFT
// =============================================================
TFT_eSPI tft = TFT_eSPI();
constexpr int SCREEN_W = 320, SCREEN_H = 240;
constexpr int FOOTER_H = 28;
constexpr int FOOTER_Y = SCREEN_H - FOOTER_H;
constexpr int DIVIDER_Y = FOOTER_Y - 4;

// Stylised fuel-pump silhouette: body + inset display + base plate + hose.
// Centered on (cx, cy), overall height ~h. The hose extends slightly to the
// right of the body — the icon is intentionally left-biased so the hose
// doesn't push the visual centre off.
void drawLogo(int cx, int cy, int h, uint16_t col) {
  int w = h * 3 / 5;
  int armW = h / 3;
  int bodyLeft = cx - (w + armW) / 2;
  int bodyTop  = cy - h / 2;
  int bodyH    = h - 4;
  int baseY    = bodyTop + bodyH;
  int baseH    = 4;
  int bodyCx   = bodyLeft + w / 2;
  int bodyRight = bodyLeft + w;

  // Body
  tft.fillRoundRect(bodyLeft, bodyTop, w, bodyH, 3, col);

  // Inset display near the top
  int dispM = 2;
  int dispH = h / 4;
  tft.fillRect(bodyLeft + dispM, bodyTop + dispM, w - 2 * dispM, dispH, COL_BG);
  // A single bright pixel-row inside the display so it reads as "a screen"
  tft.drawFastHLine(bodyLeft + dispM + 1, bodyTop + dispM + dispH / 2,
                    w - 2 * dispM - 2, col);

  // Wider base plate
  int baseW = w + 6;
  tft.fillRect(bodyCx - baseW / 2, baseY, baseW, baseH, col);

  // Hose: short arm out of the body, then a vertical hose, then a nozzle tip
  int armY  = bodyTop + dispM + dispH + 3;
  int hoseH = h / 2;
  tft.fillRect(bodyRight, armY, armW, 2, col);
  tft.fillRect(bodyRight + armW - 2, armY, 2, hoseH, col);
  tft.fillCircle(bodyRight + armW - 1, armY + hoseH + 1, 2, col);
}

String displayedName = "";
String displayedSubtitle = "";
String displayedBrandSlug = "";

// Draw `text` in font 4, wrapping onto two lines on the last space that still
// fits line 1. Returns 1 or 2 depending on how many lines were drawn.
int drawNameWrapped(const String& text, int cx, int centerY, int maxW) {
  if (tft.textWidth(text.c_str(), 4) <= maxW) {
    tft.drawString(text.c_str(), cx, centerY, 4);
    return 1;
  }

  int splitIdx = -1;
  for (int i = (int)text.length() - 1; i > 0; i--) {
    if (text.charAt(i) == ' ') {
      String first = text.substring(0, i);
      if (tft.textWidth(first.c_str(), 4) <= maxW) {
        splitIdx = i;
        break;
      }
    }
  }

  if (splitIdx < 0) {
    // Single word too wide — truncate with ellipsis
    String trimmed = text;
    while (tft.textWidth((trimmed + "...").c_str(), 4) > maxW && trimmed.length() > 4) {
      trimmed.remove(trimmed.length() - 1);
    }
    trimmed += "...";
    tft.drawString(trimmed.c_str(), cx, centerY, 4);
    return 1;
  }

  String line1 = text.substring(0, splitIdx);
  String line2 = text.substring(splitIdx + 1);
  if (tft.textWidth(line2.c_str(), 4) > maxW) {
    while (tft.textWidth((line2 + "...").c_str(), 4) > maxW && line2.length() > 4) {
      line2.remove(line2.length() - 1);
    }
    line2 += "...";
  }
  const int lineH = 30;
  tft.drawString(line1.c_str(), cx, centerY - lineH / 2, 4);
  tft.drawString(line2.c_str(), cx, centerY + lineH / 2, 4);
  return 2;
}

void drawHero() {
  tft.fillRect(0, 0, SCREEN_W, FOOTER_Y, COL_BG);
  int cx = SCREEN_W / 2;

  const BrandLogo* logo = lookupBrandLogo(displayedBrandSlug);
  if (logo != nullptr) {
    int lx = cx - logo->width / 2;
    int ly = 45 - logo->height / 2;
    // Converter writes RGB565 values MSB-first in each uint16_t. The ESP32 is
    // little-endian, so without this toggle the display reads each pixel's
    // bytes in the wrong order and colour channels end up swapped around.
    tft.setSwapBytes(true);
    tft.pushImage(lx, ly, logo->width, logo->height, logo->data);
    tft.setSwapBytes(false);
  } else {
    drawLogo(cx, 45, 55, COL_LOGO);
  }

  tft.setTextDatum(MC_DATUM);

  if (displayedName.length() == 0) {
    tft.setTextColor(COL_NAME, COL_BG);
    tft.drawString("Finding cheapest fuel...", cx, 135, 4);
    tft.drawFastHLine(20, DIVIDER_Y, SCREEN_W - 40, COL_DIVIDER);
    return;
  }

  tft.setTextColor(COL_NAME, COL_BG);
  int lines = drawNameWrapped(displayedName, cx, 120, SCREEN_W - 20);

  tft.setTextColor(COL_SUBTITLE, COL_BG);
  int subY = (lines == 2) ? 175 : 165;
  tft.drawString(displayedSubtitle.c_str(), cx, subY, 4);

  tft.drawFastHLine(20, DIVIDER_Y, SCREEN_W - 40, COL_DIVIDER);
}

void drawFooter(const char* timeStr) {
  tft.fillRect(0, FOOTER_Y, SCREEN_W, FOOTER_H, COL_BG);
  tft.setTextDatum(MC_DATUM);
  tft.setTextColor(COL_FOOTER_TEXT, COL_BG);
  tft.drawString(timeStr, SCREEN_W / 2, FOOTER_Y + FOOTER_H / 2, 2);
}

void drawCenteredStatus(const char* title, uint16_t titleCol,
                        const char* line1, const char* line2) {
  tft.fillRect(0, 0, SCREEN_W, FOOTER_Y, COL_BG);
  int cx = SCREEN_W / 2;
  drawLogo(cx, 45, 55, COL_LOGO);
  tft.setTextDatum(MC_DATUM);
  tft.setTextColor(titleCol, COL_BG);
  tft.drawString(title, cx, 110, 4);
  if (line1) { tft.setTextColor(COL_SUBTITLE, COL_BG); tft.drawString(line1, cx, 150, 2); }
  if (line2) { tft.setTextColor(COL_FOOTER_TEXT, COL_BG); tft.drawString(line2, cx, 175, 2); }
}

// =============================================================
// Preferences (NVS)
// =============================================================
Preferences prefs;

String savedSSID()      { return prefs.getString("ssid", ""); }
String savedPass()      { return prefs.getString("pass", ""); }
String savedPostcode()  { return prefs.getString("postcode", ""); }
int    savedRadius()    { return prefs.getInt("radius_mi", 5); }
String savedProxyUrl()  { return prefs.getString("proxy_url", PROXY_URL_DEFAULT); }

int    cachedUnleaded()  { return prefs.getInt("c_unl", -1); }
int    cachedDiesel()    { return prefs.getInt("c_dsl", -1); }
String cachedName()      { return prefs.getString("c_name", ""); }
String cachedSubtitle()  { return prefs.getString("c_sub", ""); }
String cachedBrandSlug() { return prefs.getString("c_bslug", ""); }

void savePrices(int u, int d, const String& name, const String& subtitle, const String& brandSlug) {
  prefs.putInt("c_unl", u);
  prefs.putInt("c_dsl", d);
  prefs.putString("c_name", name);
  prefs.putString("c_sub", subtitle);
  prefs.putString("c_bslug", brandSlug);
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
  String savedPxy = savedProxyUrl();
  String savedWifi = savedSSID();

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
    String sel = (ssid == savedWifi) ? " selected" : "";
    html += "<option" + sel + " value='" + htmlEscape(ssid) + "'>" + htmlEscape(ssid) + " (" + String(rssi) + " dBm)</option>";
  }
  html += F("</select>"
            "<label>Wi-Fi password</label>"
            "<input type=password name=pass>"
            "<div class=hint>Leave blank to keep the current password.</div>"
            "<label>UK postcode</label>"
            "<input type=text name=postcode placeholder='e.g. NR31 0DF' value='");
  html += htmlEscape(savedPc);
  html += F("' required>"
            "<div class=hint>Used to find cheapest fuel nearby.</div>"
            "<label>Search radius (miles)</label>"
            "<input type=number name=radius min=1 max=50 value='");
  html += String(savedR);
  html += F("' required>"
            "<label>Proxy URL</label>"
            "<input type=text name=proxy_url value='");
  html += htmlEscape(savedPxy);
  html += F("' required>"
            "<div class=hint>The UK-hosted proxy that fetches the fuel data.</div>"
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
  String pxy  = server.arg("proxy_url");
  pc.trim(); pc.toUpperCase();
  pxy.trim();
  while (pxy.endsWith("/")) pxy.remove(pxy.length() - 1);

  if (ssid.length() == 0 || pc.length() == 0 || radius < 1 || pxy.length() == 0) {
    server.send(400, "text/html", buildConfigPage("All fields except password are required."));
    return;
  }

  prefs.putString("ssid", ssid);
  if (pass.length() > 0) prefs.putString("pass", pass);
  prefs.putString("postcode", pc);
  prefs.putInt("radius_mi", radius);
  prefs.putString("proxy_url", pxy);

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
  tft.fillRect(0, 0, SCREEN_W, FOOTER_Y, COL_BG);
  int cx = SCREEN_W/2;
  drawLogo(cx, 40, 50, COL_LOGO);
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
// Proxy fetch
// =============================================================
CheapestStation current;
bool currentValid = false;

String urlEncode(const String& s) {
  String out;
  for (size_t i = 0; i < s.length(); i++) {
    char c = s[i];
    if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      out += c;
    } else {
      char buf[4];
      snprintf(buf, sizeof(buf), "%%%02X", (unsigned char)c);
      out += buf;
    }
  }
  return out;
}

bool fetchCheapest() {
  String url = savedProxyUrl()
             + "/cheapest?postcode=" + urlEncode(savedPostcode())
             + "&radius_miles=" + String(savedRadius());
  Serial.printf("[proxy] GET %s\n", url.c_str());

  WiFiClient client;
  HTTPClient http;
  if (!http.begin(client, url)) {
    Serial.println("[proxy] http.begin failed");
    return false;
  }
  http.setTimeout(15000);
  http.setConnectTimeout(5000);
  http.addHeader("Accept", "application/json");
  http.addHeader("User-Agent", "ESP32-FuelDisplay/2.0");

  int code = http.GET();
  String body = http.getString();
  http.end();

  Serial.printf("[proxy] -> %d, %d bytes\n", code, body.length());
  if (code != 200) {
    if (body.length()) { Serial.println(body); }
    return false;
  }

  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, body);
  if (err) {
    Serial.printf("[proxy] json parse: %s\n", err.c_str());
    return false;
  }

  current.name          = (const char*)(doc["name"]       | "");
  current.brand         = (const char*)(doc["brand"]      | "");
  current.brandSlug     = (const char*)(doc["brand_slug"] | "");
  current.town          = (const char*)(doc["town"]       | "");
  current.postcode      = (const char*)(doc["postcode"] | "");
  current.address       = (const char*)(doc["address"]  | "");
  current.isMotorway    = doc["is_motorway"]    | false;
  current.isSupermarket = doc["is_supermarket"] | false;
  double e10 = doc["e10"] | 0.0;
  double b7  = doc["b7"]  | 0.0;
  current.priceE10_ppl = (e10 > 0.0) ? (int)round(e10) : -1;
  current.priceB7_ppl  = (b7  > 0.0) ? (int)round(b7)  : -1;
  current.distanceMi   = doc["distance"]   | 0.0;
  current.updatedAt    = doc["updated_at"] | 0;
  currentValid = true;
  return true;
}

String buildSubtitle(const CheapestStation& s) {
  String out = s.town;
  if (s.distanceMi > 0) {
    char buf[24];
    snprintf(buf, sizeof(buf), " - %.1f mi", s.distanceMi);
    out += buf;
  }
  return out;
}

bool refreshFuelData(bool silent) {
  Serial.printf("[refresh] start (silent=%d)\n", silent);
  if (!silent) {
    fuel.setMode(SegMode::SPINNER);
    drawCenteredStatus("Fetching prices...", COL_NAME, "Querying proxy", nullptr);
  }

  if (!fetchCheapest()) {
    if (!silent) {
      drawCenteredStatus("Fetch failed", COL_ERROR, "Proxy unreachable", nullptr);
      delay(3000);
    }
    return false;
  }

  String shownName = current.name.length() ? current.name : current.brand;
  if (shownName.length() == 0) shownName = "(unnamed)";
  displayedName = shownName;
  displayedSubtitle = buildSubtitle(current);
  displayedBrandSlug = current.brandSlug;

  fuel.setPrices(
    current.priceE10_ppl > 0 ? current.priceE10_ppl : 0,
    current.priceB7_ppl  > 0 ? current.priceB7_ppl  : 0
  );
  fuel.setMode(SegMode::PRICES);

  savePrices(current.priceE10_ppl, current.priceB7_ppl, displayedName, displayedSubtitle, displayedBrandSlug);
  drawHero();

  Serial.printf("[refresh] ok: %s — %d/%dppl — %.1fmi\n",
                displayedName.c_str(),
                current.priceE10_ppl, current.priceB7_ppl,
                current.distanceMi);
  return true;
}

// =============================================================
// Setup & loop
// =============================================================
bool fetchTimeString(char* out, size_t outSize) {
  struct tm tm;
  if (!getLocalTime(&tm, 100)) return false;
  strftime(out, outSize, "%l:%M %p", &tm);
  if (out[0] == ' ') memmove(out, out + 1, strlen(out));
  return true;
}

// Splash shown for a few seconds after WiFi connects, so the user can see the
// LAN IP and open the settings page.
void drawBootIpSplash(const IPAddress& ip) {
  tft.fillRect(0, 0, SCREEN_W, FOOTER_Y, COL_BG);
  int cx = SCREEN_W / 2;
  drawLogo(cx, 40, 50, COL_LOGO);
  tft.setTextDatum(MC_DATUM);
  tft.setTextColor(COL_NAME, COL_BG);
  tft.drawString("Settings at", cx, 100, 4);
  tft.setTextColor(COL_ACCENT, COL_BG);
  tft.drawString(ip.toString().c_str(), cx, 140, 4);
  tft.setTextColor(COL_FOOTER_TEXT, COL_BG);
  tft.drawString("Open in a browser to change", cx, 175, 2);
  tft.drawString("postcode or radius", cx, 195, 2);
  tft.drawFastHLine(20, DIVIDER_Y, SCREEN_W - 40, COL_DIVIDER);
}

void startSettingsServer() {
  server.on("/", HTTP_GET, handleRoot);
  server.on("/save", HTTP_POST, handleSave);
  server.begin();
  Serial.printf("[web] settings at http://%s/\n", WiFi.localIP().toString().c_str());
}

void setup() {
  Serial.begin(115200);
  delay(200);
  Serial.println("[boot] petrol station display");

  tft.init();
  tft.setRotation(1);
  prefs.begin("fuelstation", false);

  // Restore cached display from last successful refresh
  displayedName = cachedName();
  displayedSubtitle = cachedSubtitle();
  displayedBrandSlug = cachedBrandSlug();
  int cu = cachedUnleaded(), cd = cachedDiesel();
  if (cu > 0) fuel.setPrices(cu, cd > 0 ? cd : 0);
  fuel.setMode(SegMode::BLANK);

  drawHero();
  drawFooter("--:--");

  if (savedSSID().length() == 0 || savedPostcode().length() == 0 || savedProxyUrl().length() == 0) {
    startCaptivePortal();
    return;
  }

  if (!tryConnectStored()) {
    startCaptivePortal();
    return;
  }

  startSettingsServer();

  // Hold the IP splash for 10s so the user can jot it down and open settings.
  // Web server stays up afterwards — the splash is just for discoverability.
  drawBootIpSplash(WiFi.localIP());
  uint32_t splashUntil = millis() + 10000;
  while ((int32_t)(splashUntil - millis()) > 0) {
    server.handleClient();
    fuel.tick();
    delay(10);
  }

  refreshFuelData(false);
}

uint32_t lastRefreshMs = 0;
bool lastRefreshOk = false;
int  consecutiveFailures = 0;

void loop() {
  fuel.tick();

  if (portalActive) {
    dnsServer.processNextRequest();
    server.handleClient();
    delay(2);
    return;
  }

  // Settings web UI stays reachable while on the home network
  server.handleClient();

  uint32_t interval = lastRefreshOk ? REFRESH_INTERVAL_MS : RETRY_AFTER_FAIL_MS;
  if (millis() - lastRefreshMs >= interval || lastRefreshMs == 0) {
    if (WiFi.status() == WL_CONNECTED) {
      lastRefreshMs = millis();
      bool silent = lastRefreshOk;  // first boot = visible, then silent
      lastRefreshOk = refreshFuelData(silent);
      if (lastRefreshOk) {
        consecutiveFailures = 0;
      } else {
        consecutiveFailures++;
        Serial.printf("[refresh] consecutive failures: %d\n", consecutiveFailures);
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          Serial.println("[refresh] too many failures, rebooting");
          delay(1000);
          ESP.restart();
        }
      }
    }
  }

  // 1 Hz footer tick
  static uint32_t lastUiTick = 0;
  if (millis() - lastUiTick < 1000) { delay(20); return; }
  lastUiTick = millis();

  char timeStr[16] = "--:--";
  if (WiFi.status() == WL_CONNECTED) fetchTimeString(timeStr, sizeof(timeStr));
  drawFooter(timeStr);
}
