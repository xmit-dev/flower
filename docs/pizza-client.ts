import { FlowerClient } from "@flower-js/sdk";
import type pizza from "./pizza.ts";

const client = new FlowerClient<typeof pizza>("http://127.0.0.1:7101");
await client.mutate("pizza.order", { id: "first-pizza", topping: "mushroom" }, {
  requestId: "first-pizza", retry: true, // Lost replies retry with the same ID.
});

for await (const { value } of client.subscribe("pizza.board")) {
  console.log(value); // { orders: 1, mushroom: 1 }
}
