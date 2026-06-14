/*
 * Atlas  <->  Home Assistant   —   connection layer
 * --------------------------------------------------
 * This is the ONLY file in Atlas that talks to Home Assistant.
 * Everything else in the app calls these methods — never HA (or ecobee) directly.
 *
 * Why this matters: if you ever swap ecobee for different thermostats, only the
 * Home Assistant side changes. This file — and your whole app — stay the same.
 *
 * It uses Home Assistant's WebSocket API, which works through your Nabu Casa
 * URL and is NOT blocked by browser CORS rules (unlike the REST API).
 *
 * Usage:
 *   const ha = new AtlasHA("https://YOUR.ui.nabu.casa", "YOUR_LONG_LIVED_TOKEN");
 *   await ha.connect();
 *   const zones = await ha.getThermostats();
 *   await ha.setTemperature("climate.gym", 72);
 */

class AtlasHA {
  constructor(baseUrl, token) {
    // turn https://x.ui.nabu.casa  ->  wss://x.ui.nabu.casa/api/websocket
    this.wsUrl = baseUrl.trim().replace(/^http/, "ws").replace(/\/+$/, "") + "/api/websocket";
    this.token = token.trim();
    this.ws = null;
    this.connected = false;
    this._id = 1;
    this._pending = {};        // message id -> {resolve, reject}
    this._stateListeners = []; // callbacks for live state changes
    this._closeListeners = []; // fired if the socket drops AFTER a successful login
    this.subscribed = false;   // whether we've subscribed to state_changed events
  }

  /* Register a callback fired if the connection drops after a successful login.
     Lets the app auto-reconnect silently instead of falling back to the login form. */
  onClose(cb) { this._closeListeners.push(cb); }

  /* Open the connection and log in. Resolves once authenticated. */
  connect() {
    return new Promise((resolve, reject) => {
      let ws, opened = false, settled = false;
      const done = (fn, val) => { if (!settled) { settled = true; clearTimeout(timer); fn(val); } };
      try { ws = new WebSocket(this.wsUrl); }
      catch (e) { return reject(new Error("Bad Home Assistant URL: " + this.wsUrl)); }
      this.ws = ws;

      // 10s safety net
      const timer = setTimeout(() => {
        done(reject, new Error("Timed out reaching " + this.wsUrl +
          " — a browser extension (ad/privacy blocker) or your network is likely blocking WebSockets."));
      }, 10000);

      ws.onopen = () => { opened = true; };

      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        switch (msg.type) {
          case "auth_required":
            ws.send(JSON.stringify({ type: "auth", access_token: this.token }));
            break;
          case "auth_ok":
            this.connected = true;
            done(resolve);
            break;
          case "auth_invalid": {
            // The token itself is bad — the app SHOULD re-prompt for this one.
            const err = new Error("Connected, but Home Assistant rejected the token. Generate a fresh one and re-paste it (watch for missing characters).");
            err.authFailed = true;
            done(reject, err);
            break;
          }
          case "result": {
            const p = this._pending[msg.id];
            if (p) {
              msg.success ? p.resolve(msg.result)
                          : p.reject(new Error(msg.error && msg.error.message || "Home Assistant error"));
              delete this._pending[msg.id];
            }
            break;
          }
          case "event": {
            const data = msg.event && msg.event.data;
            if (data) this._stateListeners.forEach((fn) => fn(data));
            break;
          }
        }
      };

      ws.onerror = () => done(reject, new Error(
        (opened ? "Connection dropped after opening" : "Couldn't open a connection") +
        " to " + this.wsUrl + ". " +
        (opened ? "" : "This browser is blocking it — usually an ad/privacy extension, or open it in a clean browser profile.")
      ));

      ws.onclose = (e) => {
        const wasConnected = this.connected;
        this.connected = false;
        this.subscribed = false;
        if (!opened) {
          done(reject, new Error(
            "Connection closed before it opened (code " + e.code + "). " +
            (e.code === 1006 ? "Code 1006 = a browser extension or network silently blocked the WebSocket." : "")
          ));
        } else if (wasConnected) {
          // We were live and the socket dropped (wifi blip, HA restart, laptop sleep).
          // Tell the app so it can reconnect silently — no re-login needed.
          this._closeListeners.forEach((fn) => { try { fn(e); } catch (_) {} });
        }
      };
    });
  }

  /* internal: send a command and wait for its result */
  _send(payload) {
    return new Promise((resolve, reject) => {
      if (!this.connected) return reject(new Error("Not connected to Home Assistant yet."));
      const id = this._id++;
      this._pending[id] = { resolve, reject };
      this.ws.send(JSON.stringify(Object.assign({ id }, payload)));
    });
  }

  /* Every entity in the house (lights, sensors, thermostats, ...) */
  getStates() { return this._send({ type: "get_states" }); }

  /* Just the thermostats (climate.* entities) */
  async getThermostats() {
    const states = await this.getStates();
    return states.filter((s) => s.entity_id.startsWith("climate."));
  }

  /* Run any Home Assistant action */
  callService(domain, service, data = {}, target = {}) {
    return this._send({ type: "call_service", domain, service, service_data: data, target });
  }

  /* Set a zone's target temperature, e.g. setTemperature("climate.gym", 72) */
  setTemperature(entityId, temp) {
    return this.callService("climate", "set_temperature", { temperature: temp }, { entity_id: entityId });
  }

  /* Change mode: "cool" | "heat" | "heat_cool" | "off" */
  setHvacMode(entityId, mode) {
    return this.callService("climate", "set_hvac_mode", { hvac_mode: mode }, { entity_id: entityId });
  }

  /* Get one entity's current state object (or null) */
  async getEntity(entityId) {
    const states = await this.getStates();
    return states.find((s) => s.entity_id === entityId) || null;
  }

  /* Live updates: cb({entity_id, new_state, old_state}) fires on any change */
  async onStateChange(cb) {
    this._stateListeners.push(cb);
    await this._send({ type: "subscribe_events", event_type: "state_changed" });
    this.subscribed = true;
  }
}

// make it available to the page / the rest of Atlas
if (typeof window !== "undefined") window.AtlasHA = AtlasHA;
