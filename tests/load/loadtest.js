import http from 'k6/http';
import { check, sleep } from 'k6';

// 1. Configure the load test stages
export const options = {
  stages: [
    { duration: '10s', target: 5 },  // Gentle ramp up to 5 virtual users
    { duration: '30s', target: 5 },  // Stay at 5 virtual users for 30 seconds
    { duration: '5s', target: 0 },  // Ramp down to 0 virtual users
  ],
  thresholds: {
    // We want 95% of requests to complete within 2000ms (2 seconds)
    http_req_duration: ['p(95)<2000'],
    // We want less than 1% of requests to fail
    http_req_failed: ['rate<0.01'], 
  },
};

// 2. Define what each Virtual User (VU) does
export default function () {
  // Use the API container's name from docker-compose if running in the same network,
  // or localhost if running k6 from the host machine. 
  // Since we will run k6 in a docker container attached to the letsgossip_default network, we use 'api'.
  const url = 'http://api:8080/jobs';
  
  const payload = JSON.stringify({
    Type: "EmailJob",
    Payload: "{\"recipient\":\"test@example.com\", \"body\":\"Load test!\"}",
    Priority: 5
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  // Send the POST request
  const res = http.post(url, payload, params);
  
  // Verify the response was successful (201 Created)
  check(res, {
    'is status 201': (r) => r.status === 201,
  });
  
  // Short pause to simulate realistic traffic pacing
  sleep(0.1); 
}
