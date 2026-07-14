import { clearAuth, getApiBase, getApiKey } from "./auth";
import type { CallStatus } from "@/types/call";
import type { SessionInfo, SessionState } from "@/types/session";

type CallListRow = {
  sessionId: string;
  callId: string;
  owner: string | null;
  direction: "outbound" | "inbound";
  peer: string;
  startedAt: number;
  status: CallStatus;
  endedAt?: number;
  endReason?: string;
};

export type BrokerEvent =
  | { type: "session-list"; sessions: SessionInfo[] }
  | { type: "session-qr"; sessionId: string; qr: string }
  | { type: "auth-state"; sessionId: string; paired: boolean; state: SessionState; qr?: string }
  | { type: "call-list"; calls: CallListRow[] }
  | { type: "call-status"; sessionId: string; id: string; owner: string | null; status: CallStatus; peer: string; startedAt: number }
  | { type: "call-ended"; sessionId: string; id: string; owner: string | null; reason: string; endedAt: number }
  | { type: "incoming"; sessionId: string; id: string; peer: string; offeredAt: number }
  | { type: "incoming-claimed"; sessionId: string; id: string; owner: string };

type Listener = (ev: BrokerEvent) => void;

class EventStream {
  #controller: AbortController | null = null;
  #retryTimer: ReturnType<typeof setTimeout> | null = null;
  #clientId = "";
  #listeners = new Set<Listener>();

  connect(clientId: string): void {
    if (this.#controller || this.#retryTimer) return;
    this.#clientId = clientId;
    void this.#open();
  }

  on(l: Listener): () => void {
    this.#listeners.add(l);
    return () => this.#listeners.delete(l);
  }

  close(): void {
    this.#clientId = "";
    this.#controller?.abort();
    this.#controller = null;
    if (this.#retryTimer) clearTimeout(this.#retryTimer);
    this.#retryTimer = null;
  }

  async #open(): Promise<void> {
    if (!this.#clientId || this.#controller) return;

    const controller = new AbortController();
    this.#controller = controller;
    try {
      const url = `${getApiBase()}/api/events?clientId=${encodeURIComponent(this.#clientId)}`;
      const response = await fetch(url, {
        headers: {
          Accept: "text/event-stream",
          "X-API-Key": getApiKey(),
        },
        signal: controller.signal,
      });
      if (response.status === 401) {
        this.#clientId = "";
        clearAuth();
        location.reload();
        return;
      }
      if (!response.ok || !response.body) throw new Error(`event stream ${response.status}`);

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (!controller.signal.aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        let boundary = buffer.indexOf("\n\n");
        while (boundary >= 0) {
          this.#dispatch(buffer.slice(0, boundary));
          buffer = buffer.slice(boundary + 2);
          boundary = buffer.indexOf("\n\n");
        }
      }
    } catch {
      // A queda e tratada pelo reconnect limitado abaixo.
    } finally {
      if (this.#controller === controller) this.#controller = null;
      if (!controller.signal.aborted && this.#clientId) {
        this.#retryTimer = setTimeout(() => {
          this.#retryTimer = null;
          void this.#open();
        }, 1000);
      }
    }
  }

  #dispatch(frame: string): void {
    const data = frame
      .split("\n")
      .filter(line => line.startsWith("data:"))
      .map(line => line.slice(5).trimStart())
      .join("\n");
    if (!data) return;

    try {
      const parsed: BrokerEvent = JSON.parse(data);
      for (const listener of this.#listeners) listener(parsed);
    } catch {
      // Ignora frames incompletos ou eventos desconhecidos.
    }
  }
}

export const eventStream = new EventStream();
