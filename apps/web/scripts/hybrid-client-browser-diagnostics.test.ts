// @effect-diagnostics nodeBuiltinImport:off - offline privacy/integrity checks for controlled browser evidence.
import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import {
  browserDiagnosticLimits,
  browserDiagnosticUrl,
  createBrowserDiagnostics,
} from "./hybrid-client-browser-diagnostics.ts";

NodeTest.test("retains origin and pathname while removing credentials, query and fragment", () => {
  const value = browserDiagnosticUrl(
    "https://PRIVATE_USER:PRIVATE_PASSWORD@example.test:8443/api/thread?token=PRIVATE_QUERY#PRIVATE_FRAGMENT",
  );
  NodeAssert.deepEqual(value, {
    status: "captured",
    origin: "https://example.test:8443",
    pathname: "/api/thread",
    truncated: false,
  });
  NodeAssert.doesNotMatch(JSON.stringify(value), /PRIVATE_|token|password|username/iu);
  NodeAssert.equal(
    browserDiagnosticUrl("wss://example.test/ws?session=PRIVATE_QUERY").origin,
    "wss://example.test",
  );
});

NodeTest.test("invalid, unsupported and over-limit URLs expose a reason without raw input", () => {
  const values = [
    browserDiagnosticUrl(undefined),
    browserDiagnosticUrl("PRIVATE_INVALID_URL"),
    browserDiagnosticUrl("data:text/plain,PRIVATE_DATA"),
    browserDiagnosticUrl("file:///PRIVATE_PATH"),
    browserDiagnosticUrl(
      "https://example.test/" + "x".repeat(browserDiagnosticLimits.inputUrlCharacters),
    ),
  ];
  NodeAssert.deepEqual(
    values.map((value) => value.status),
    ["missing", "invalid", "unsupported-protocol", "unsupported-protocol", "over-limit"],
  );
  NodeAssert.ok(values.every((value) => value.origin === null && value.pathname === null));
  NodeAssert.doesNotMatch(JSON.stringify(values), /PRIVATE_|example\.test/u);
});

NodeTest.test(
  "caps events and URL fields, exposes truncation, and never spreads unknown input",
  () => {
    const recorder = createBrowserDiagnostics();
    const extra = {
      body: "PRIVATE_BODY",
      headers: "PRIVATE_HEADERS",
      consoleText: "PRIVATE_CONSOLE_TEXT",
    };
    recorder.record({
      kind: "http-error",
      urlSource: "request",
      status: 502,
      method: "GET",
      resourceType: "fetch",
      url: "http://localhost/" + "x".repeat(2_000) + "?PRIVATE_QUERY#PRIVATE_FRAGMENT",
      ...extra,
    });
    for (let index = 0; index < browserDiagnosticLimits.events + 4; index++) {
      recorder.record({
        kind: "request-failed",
        urlSource: "request",
        url: "http://localhost/api",
        failureCode: "net::ERR_CONNECTION_REFUSED",
      });
    }
    NodeAssert.equal(recorder.state.events.length, browserDiagnosticLimits.events);
    NodeAssert.equal(recorder.state.droppedEvents, 5);
    NodeAssert.equal(recorder.state.truncatedValues, 1);
    NodeAssert.equal(recorder.state.truncated, true);
    NodeAssert.equal(
      recorder.state.events[0]?.url.pathname?.length,
      browserDiagnosticLimits.pathnameCharacters,
    );
    NodeAssert.equal(recorder.state.events[1]?.failureCode, "net::ERR_CONNECTION_REFUSED");
    NodeAssert.doesNotMatch(JSON.stringify(recorder.state), /PRIVATE_/u);
  },
);

NodeTest.test(
  "records explicit restart phases and associates errors with their current step",
  () => {
    let milliseconds = 0;
    const recorder = createBrowserDiagnostics(() => ({
      timestampMs: 1_700_000_000_000 + milliseconds,
      monotonicMs: milliseconds,
    }));
    const phases = [
      "owned server restart: stopping",
      "owned server restart: stopped",
      "owned server restart: starting",
      "owned server restart: ready",
    ];
    for (const phase of phases) {
      milliseconds += 10;
      recorder.phase(phase);
      recorder.record({
        kind: "console-error",
        urlSource: "console-location",
        url: "http://localhost/api?secret=PRIVATE_QUERY",
        browserErrorIndex: recorder.state.events.length,
      });
    }
    NodeAssert.deepEqual(
      recorder.state.phases.map((phase) => phase.currentStep),
      phases,
    );
    NodeAssert.deepEqual(
      recorder.state.events.map((event) => event.currentStep),
      phases,
    );
    NodeAssert.deepEqual(
      recorder.state.events.map((event) => event.elapsedMs),
      [10, 20, 30, 40],
    );
    NodeAssert.deepEqual(
      recorder.state.events.map((event) => event.browserErrorIndex),
      [0, 1, 2, 3],
    );
    NodeAssert.equal(recorder.state.events[0]?.timestamp, "2023-11-14T22:13:20.010Z");
  },
);

NodeTest.test(
  "phase floods remain bounded while subsequent errors retain the latest bounded step",
  () => {
    const recorder = createBrowserDiagnostics();
    for (let index = 0; index < browserDiagnosticLimits.phases; index++)
      recorder.phase("step " + index);
    recorder.phase("z".repeat(browserDiagnosticLimits.stepCharacters + 1));
    recorder.record({
      kind: "request-failed",
      urlSource: "request",
      url: "about:blank",
      failureCode: "HTTP error from https://PRIVATE_URL/?token=PRIVATE_QUERY",
      method: "PRIVATE_METHOD",
      resourceType: "PRIVATE_RESOURCE",
    });
    NodeAssert.equal(recorder.state.phases.length, browserDiagnosticLimits.phases);
    NodeAssert.equal(recorder.state.droppedPhases, 1);
    NodeAssert.equal(recorder.state.truncatedValues, 1);
    NodeAssert.equal(
      recorder.state.events[0]?.currentStep.length,
      browserDiagnosticLimits.stepCharacters,
    );
    NodeAssert.equal(recorder.state.events[0]?.stepTruncated, true);
    NodeAssert.equal(recorder.state.events[0]?.failureCode, null);
    NodeAssert.equal(recorder.state.events[0]?.method, null);
    NodeAssert.equal(recorder.state.events[0]?.resourceType, null);
    NodeAssert.doesNotMatch(JSON.stringify(recorder.state), /PRIVATE_/u);
  },
);
