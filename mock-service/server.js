const express = require('express');
const app = express();
app.use(express.json());

app.post('/notify', (req, res) => {
  const idempotencyKey = req.headers['idempotency-key'];
  console.log(`[Mock Service] Received notification request. Idempotency-Key: ${idempotencyKey}`);

  // Simulate gateway failure roll (70% success, 20% transient fail, 10% slow)
  const roll = Math.random();
  if (roll < 0.2) {
    console.log('[Mock Service] Simulating transient 500 failure');
    return res.status(500).json({ error: 'Gateway unavailable' });
  }

  console.log('[Mock Service] Notification delivered successfully');
  return res.status(200).json({ status: 'DELIVERED' });
});

app.listen(4000, () => {
  console.log('Mock Notification Service running on port 4000');
});