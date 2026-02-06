import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: __ENV.RATE ? parseInt(__ENV.RATE, 10) : 50, // requests per second
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: __ENV.VUS ? parseInt(__ENV.VUS, 10) : 50,
      maxVUs: __ENV.MAX_VUS ? parseInt(__ENV.MAX_VUS, 10) : 200,
    },
  },
};

export default function () {
  const url = __ENV.URL || "http://ingestion-api:8000/v1/events";
  const now = new Date().toISOString();
  const id = `${__VU}-${__ITER}-${Date.now()}`;

  const payload = JSON.stringify({
    event_id: `load-${id}`,
    timestamp: now,
    source: "k6",
    type: "user.action",
    payload: { action: "load_test", vu: __VU, iter: __ITER },
  });

  const params = { headers: { "Content-Type": "application/json" } };
  const res = http.post(url, payload, params);

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(0.01);
}
