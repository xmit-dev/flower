import { FlowerClient } from "@flower-js/sdk";
import type terrarium from "./terrarium.ts";

const client = new FlowerClient<typeof terrarium>("http://127.0.0.1:7101");
await client.mutate("garden.plant", { garden: "moon-garden", id: "luna" }, {
  requestId: "plant-moon-garden-luna", retry: true, // Retries reuse this ID.
});

for await (const { value } of client.subscribe("garden.view", "moon-garden")) {
  console.log(value);
  // { spacesLeft: 11, blooming: 0, flowers: { luna: "🌱" } }
  // After 5s: a bloom. After 35s: an empty spot, ready for another seed.
}
