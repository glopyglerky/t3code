export const browserDiagnosticLimits = Object.freeze({
  events: 128,
  phases: 32,
  stepCharacters: 160,
  inputUrlCharacters: 8_192,
  originCharacters: 512,
  pathnameCharacters: 1_024,
});

type UrlObservation = {
  status: "captured" | "missing" | "invalid" | "unsupported-protocol" | "over-limit";
  origin: string | null;
  pathname: string | null;
  truncated: boolean;
};

export function browserDiagnosticUrl(raw: string | null | undefined): UrlObservation {
  const empty = { origin: null, pathname: null, truncated: false };
  if (!raw) return { ...empty, status: "missing" };
  if (raw.length > browserDiagnosticLimits.inputUrlCharacters) {
    return { ...empty, status: "over-limit", truncated: true };
  }
  try {
    const url = new URL(raw);
    if (!["http:", "https:", "ws:", "wss:"].includes(url.protocol)) {
      return { ...empty, status: "unsupported-protocol" };
    }
    return {
      status: "captured",
      // URL.origin excludes userinfo. Never retain href, search, hash or credentials.
      origin: url.origin.slice(0, browserDiagnosticLimits.originCharacters),
      pathname: url.pathname.slice(0, browserDiagnosticLimits.pathnameCharacters),
      truncated:
        url.origin.length > browserDiagnosticLimits.originCharacters ||
        url.pathname.length > browserDiagnosticLimits.pathnameCharacters,
    };
  } catch {
    return { ...empty, status: "invalid" };
  }
}

type EventInput = {
  kind: "console-error" | "page-error" | "request-failed" | "http-error";
  url: string | null | undefined;
  urlSource: "console-location" | "page" | "request";
  browserErrorIndex?: number;
  status?: number;
  method?: string;
  resourceType?: string;
  failureCode?: string | undefined;
};
type Clock = () => { timestampMs: number; monotonicMs: number };
type Phase = {
  timestamp: string;
  elapsedMs: number;
  currentStep: string;
  stepTruncated: boolean;
};
type Event = Phase & {
  kind: EventInput["kind"];
  urlSource: EventInput["urlSource"];
  url: UrlObservation;
  browserErrorIndex: number | null;
  status: number | null;
  method: string | null;
  resourceType: string | null;
  failureCode: string | null;
};

const methods = new Set([
  "GET",
  "HEAD",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "OPTIONS",
  "CONNECT",
  "TRACE",
]);
const resourceTypes = new Set([
  "document",
  "stylesheet",
  "image",
  "media",
  "font",
  "script",
  "texttrack",
  "xhr",
  "fetch",
  "eventsource",
  "websocket",
  "manifest",
  "other",
]);

export function createBrowserDiagnostics(
  clock: Clock = () => ({ timestampMs: Date.now(), monotonicMs: performance.now() }),
) {
  const started = clock().monotonicMs;
  let currentStep = "application startup";
  let stepTruncated = false;
  const events: Event[] = [];
  const phases: Phase[] = [];
  const state = {
    schemaVersion: 1,
    scope:
      "Bounded diagnostics for the controlled browser flow; no pass/fail policy change. URLs contain origin/pathname only; no query, hash, userinfo, headers or body. Console text stays solely in the existing browserErrors array.",
    limits: browserDiagnosticLimits,
    truncated: false,
    droppedEvents: 0,
    droppedPhases: 0,
    truncatedValues: 0,
    events,
    phases,
  };
  const snapshot = (): Phase => {
    const now = clock();
    return {
      timestamp: new Date(now.timestampMs).toISOString(),
      elapsedMs: Math.max(0, now.monotonicMs - started),
      currentStep,
      stepTruncated,
    };
  };
  return {
    state,
    phase(step: string): void {
      currentStep = step
        .slice(0, browserDiagnosticLimits.stepCharacters)
        .replace(/[\u0000-\u001f\u007f]/gu, " ");
      stepTruncated = step.length > browserDiagnosticLimits.stepCharacters;
      if (stepTruncated) {
        state.truncated = true;
        state.truncatedValues++;
      }
      if (phases.length >= browserDiagnosticLimits.phases) {
        state.truncated = true;
        state.droppedPhases++;
        return;
      }
      phases.push(snapshot());
    },
    record(input: EventInput): void {
      if (events.length >= browserDiagnosticLimits.events) {
        state.truncated = true;
        state.droppedEvents++;
        return;
      }
      const url = browserDiagnosticUrl(input.url);
      if (url.truncated) {
        state.truncated = true;
        state.truncatedValues++;
      }
      events.push({
        ...snapshot(),
        kind: input.kind,
        urlSource: input.urlSource,
        url,
        browserErrorIndex:
          typeof input.browserErrorIndex === "number" &&
          Number.isSafeInteger(input.browserErrorIndex) &&
          input.browserErrorIndex >= 0
            ? input.browserErrorIndex
            : null,
        status:
          typeof input.status === "number" &&
          Number.isInteger(input.status) &&
          input.status >= 400 &&
          input.status <= 999
            ? input.status
            : null,
        method: input.method !== undefined && methods.has(input.method) ? input.method : null,
        resourceType:
          input.resourceType !== undefined && resourceTypes.has(input.resourceType)
            ? input.resourceType
            : null,
        // Chromium's symbolic network code is useful; never duplicate arbitrary error text.
        failureCode:
          input.failureCode !== undefined &&
          input.failureCode.length <= 96 &&
          /^net::ERR_[A-Z0-9_]+$/u.test(input.failureCode)
            ? input.failureCode
            : null,
      });
    },
  };
}
