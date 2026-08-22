import React, { useState, useEffect, useRef, useCallback } from 'react';

// Environment variable defaults or browser protocol auto-detection
const DEFAULT_ENV_WS = import.meta.env.VITE_BACKEND_WS_URL || '';
const isHttps = typeof window !== 'undefined' && window.location.protocol === 'https:';
const defaultProtocol = isHttps ? 'wss' : 'ws';

export default function App() {
  // Connection Configuration
  const [protocol, setProtocol] = useState(defaultProtocol);
  const [host, setHost] = useState(DEFAULT_ENV_WS ? '' : 'localhost');
  const [port, setPort] = useState(DEFAULT_ENV_WS ? '' : '8000');
  const [customWsUrl, setCustomWsUrl] = useState(DEFAULT_ENV_WS);
  const [useCustomUrl, setUseCustomUrl] = useState(Boolean(DEFAULT_ENV_WS));
  const [clientId, setClientId] = useState('client_1');

  // Connection State: 'disconnected' | 'connecting' | 'connected' | 'reconnecting'
  const [status, setStatus] = useState('disconnected');
  const [activeInstance, setActiveInstance] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');

  // Event Input Form
  const [eventType, setEventType] = useState('chat.message');
  const [payloadText, setPayloadText] = useState('Hello from Web client!');

  // Real-time Event Feed
  const [events, setEvents] = useState([]);

  // WebSocket & Reconnection References
  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const reconnectDelayRef = useRef(1000); // start with 1s
  const manuallyClosedRef = useRef(false);

  // Compute Active Target URL
  const computeWsUrl = useCallback(() => {
    if (useCustomUrl && customWsUrl.trim()) {
      let base = customWsUrl.trim();
      if (base.endsWith('/')) base = base.slice(0, -1);
      return base.includes('/ws/') ? base : `${base}/ws/${encodeURIComponent(clientId.trim())}`;
    }
    const portPart = port ? `:${port}` : '';
    return `${protocol}://${host}${portPart}/ws/${encodeURIComponent(clientId.trim())}`;
  }, [useCustomUrl, customWsUrl, protocol, host, port, clientId]);

  const activeWsUrl = computeWsUrl();

  const appendFeedItem = useCallback((item) => {
    setEvents((prev) => [
      {
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
        receivedAt: new Date().toLocaleTimeString(),
        ...item,
      },
      ...prev.slice(0, 99), // keep latest 100 events
    ]);
  }, []);

  const connectWebSocket = useCallback(() => {
    if (!clientId.trim()) {
      setErrorMessage('Client ID cannot be empty');
      return;
    }

    setErrorMessage('');
    setStatus((prev) => (prev === 'disconnected' ? 'connecting' : 'reconnecting'));
    manuallyClosedRef.current = false;

    const targetUrl = computeWsUrl();

    try {
      const ws = new WebSocket(targetUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        reconnectDelayRef.current = 1000; // Reset exponential backoff
        appendFeedItem({
          type: 'system',
          title: 'WebSocket Connected',
          details: `Connected to ${targetUrl}`,
        });
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          // Handle Heartbeat Ping
          if (data.type === 'ping') {
            ws.send(JSON.stringify({ type: 'pong' }));
            return;
          }

          // Handle Connected Ack
          if (data.type === 'ack') {
            if (data.instance_id) {
              setActiveInstance(data.instance_id);
            }
            appendFeedItem({
              type: 'ack',
              title: 'Server ACK',
              details: data.message || JSON.stringify(data),
              instanceId: data.instance_id,
            });
            return;
          }

          // Handle Distributed Redis Events
          if (data.type === 'event') {
            appendFeedItem({
              type: 'event',
              eventType: data.event_type,
              sourceClientId: data.source_client_id,
              instanceId: data.published_by_instance,
              payload: data.payload,
            });
            return;
          }

          // Other messages
          appendFeedItem({
            type: 'message',
            title: data.type || 'Message',
            details: JSON.stringify(data),
          });
        } catch {
          appendFeedItem({
            type: 'raw',
            title: 'Raw Frame',
            details: event.data,
          });
        }
      };

      ws.onerror = (err) => {
        console.error('WebSocket Error:', err);
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (manuallyClosedRef.current) {
          setStatus('disconnected');
          setActiveInstance(null);
          appendFeedItem({
            type: 'system',
            title: 'Disconnected',
            details: 'Connection closed by user',
          });
        } else {
          // Automatic Reconnection with Exponential Backoff
          setStatus('reconnecting');
          const delay = reconnectDelayRef.current;
          appendFeedItem({
            type: 'system',
            title: 'Connection Lost',
            details: `Reconnecting in ${delay / 1000}s...`,
          });
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectDelayRef.current = Math.min(reconnectDelayRef.current * 2, 30000);
            connectWebSocket();
          }, delay);
        }
      };
    } catch (err) {
      setErrorMessage(`Failed to initialize WebSocket: ${err.message}`);
      setStatus('disconnected');
    }
  }, [clientId, computeWsUrl, appendFeedItem]);

  const disconnectWebSocket = () => {
    manuallyClosedRef.current = true;
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    setStatus('disconnected');
    setActiveInstance(null);
  };

  const handleSendEvent = (e) => {
    e.preventDefault();
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      setErrorMessage('Cannot send: WebSocket is not connected.');
      return;
    }

    setErrorMessage('');

    let parsedPayload;
    try {
      parsedPayload = payloadText.startsWith('{') ? JSON.parse(payloadText) : { message: payloadText };
    } catch {
      parsedPayload = { message: payloadText };
    }

    const message = {
      type: 'publish',
      event_type: eventType.trim() || 'custom.event',
      payload: parsedPayload,
    };

    wsRef.current.send(JSON.stringify(message));
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      manuallyClosedRef.current = true;
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div>
          <h1 className="title">⚡ Real-Time Streaming Gateway</h1>
          <p className="subtitle">
            Distributed WebSocket Event Streaming powered by FastAPI &amp; Redis Pub/Sub
          </p>
        </div>
        <div className="header-status">
          <span className={`status-badge status-${status}`}>
            <span className="status-dot"></span>
            {status.toUpperCase()}
          </span>
          {activeInstance && (
            <span className="instance-badge" title="Connected FastAPI Instance">
              Node: {activeInstance}
            </span>
          )}
        </div>
      </header>

      {/* Main Grid */}
      <main className="main-grid">
        {/* Left Column: Controls */}
        <section className="panel">
          <div className="panel-header-row">
            <h2 className="panel-title">Connection Settings</h2>
            <button
              type="button"
              className="btn-mode-toggle"
              onClick={() => setUseCustomUrl(!useCustomUrl)}
              disabled={status === 'connected' || status === 'connecting'}
            >
              {useCustomUrl ? 'Use Host/Port Mode' : 'Use Direct URL Mode'}
            </button>
          </div>

          {!useCustomUrl ? (
            <>
              <div className="form-group">
                <label className="form-label">Protocol, Host &amp; Port</label>
                <div className="input-group">
                  <select
                    className="input input-protocol"
                    value={protocol}
                    disabled={status === 'connected' || status === 'connecting'}
                    onChange={(e) => setProtocol(e.target.value)}
                  >
                    <option value="ws">ws://</option>
                    <option value="wss">wss:// (TLS)</option>
                  </select>
                  <input
                    type="text"
                    className="input"
                    value={host}
                    disabled={status === 'connected' || status === 'connecting'}
                    onChange={(e) => setHost(e.target.value)}
                    placeholder="localhost or your-domain.com"
                  />
                  <input
                    type="number"
                    className="input input-short"
                    value={port}
                    disabled={status === 'connected' || status === 'connecting'}
                    onChange={(e) => setPort(e.target.value)}
                    placeholder="8000"
                  />
                </div>
                <span className="field-hint">
                  Local ports: <code>8000</code>, <code>8001</code>, <code>8002</code> | Cloud: leave port blank for standard 80/443
                </span>
              </div>
            </>
          ) : (
            <div className="form-group">
              <label className="form-label">Full Backend WebSocket URL / Base</label>
              <input
                type="text"
                className="input"
                value={customWsUrl}
                disabled={status === 'connected' || status === 'connecting'}
                onChange={(e) => setCustomWsUrl(e.target.value)}
                placeholder="wss://your-backend.onrender.com"
              />
              <span className="field-hint">
                E.g. <code>wss://streaming-gateway.onrender.com</code> or <code>wss://api.yourdomain.com</code>
              </span>
            </div>
          )}

          <div className="form-group">
            <label className="form-label">Client ID</label>
            <input
              type="text"
              className="input"
              value={clientId}
              disabled={status === 'connected' || status === 'connecting'}
              onChange={(e) => setClientId(e.target.value)}
              placeholder="e.g. alice, bob, dashboard_1"
            />
          </div>

          <div className="form-group">
            <label className="form-label">Target WebSocket URL</label>
            <div className="url-display">
              <code>{activeWsUrl}</code>
            </div>
          </div>

          <div className="button-group">
            {status === 'disconnected' ? (
              <button className="btn btn-primary" onClick={connectWebSocket}>
                Connect WebSocket
              </button>
            ) : (
              <button className="btn btn-danger" onClick={disconnectWebSocket}>
                Disconnect
              </button>
            )}
          </div>

          {errorMessage && <div className="alert alert-error">{errorMessage}</div>}

          <hr className="divider" />

          <h2 className="panel-title">Publish Event</h2>
          <form onSubmit={handleSendEvent}>
            <div className="form-group">
              <label className="form-label">Event Type</label>
              <input
                type="text"
                className="input"
                value={eventType}
                disabled={status !== 'connected'}
                onChange={(e) => setEventType(e.target.value)}
                placeholder="e.g. order.created, chat.message"
              />
            </div>

            <div className="form-group">
              <label className="form-label">Message Payload (Text or JSON)</label>
              <textarea
                className="textarea"
                rows={3}
                value={payloadText}
                disabled={status !== 'connected'}
                onChange={(e) => setPayloadText(e.target.value)}
                placeholder='e.g. Hello world or {"amount": 100}'
              />
            </div>

            <button
              type="submit"
              className="btn btn-success"
              disabled={status !== 'connected'}
            >
              Send Event via WebSocket
            </button>
          </form>
        </section>

        {/* Right Column: Real-Time Feed */}
        <section className="panel feed-panel">
          <div className="feed-header">
            <h2 className="panel-title">Real-Time Event Stream ({events.length})</h2>
            {events.length > 0 && (
              <button className="btn-text" onClick={() => setEvents([])}>
                Clear Feed
              </button>
            )}
          </div>

          <div className="event-list">
            {events.length === 0 ? (
              <div className="empty-state">
                <p>No events received yet.</p>
                <span className="field-hint">
                  Connect to the gateway or publish an event to see real-time updates.
                </span>
              </div>
            ) : (
              events.map((item) => (
                <div key={item.id} className={`event-card event-card-${item.type}`}>
                  <div className="event-card-header">
                    <div className="event-type-badge">
                      {item.type === 'event' ? (
                        <strong>⚡ {item.eventType}</strong>
                      ) : (
                        <span>{item.title || item.type}</span>
                      )}
                    </div>
                    <div className="event-meta">
                      {item.instanceId && (
                        <span className="node-tag">Node: {item.instanceId}</span>
                      )}
                      {item.sourceClientId && (
                        <span className="client-tag">From: {item.sourceClientId}</span>
                      )}
                      <span className="timestamp">{item.receivedAt}</span>
                    </div>
                  </div>

                  <div className="event-body">
                    {item.payload ? (
                      <pre className="payload-json">
                        {typeof item.payload === 'object'
                          ? JSON.stringify(item.payload, null, 2)
                          : item.payload}
                      </pre>
                    ) : (
                      <p className="event-detail">{item.details}</p>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </section>
      </main>
    </div>
  );
}
