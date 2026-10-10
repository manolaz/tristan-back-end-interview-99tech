import 'dotenv/config';
import { database } from './database.js';
import app from './app.js';

const port = Number(process.env.PORT ?? 3000);

if (process.env.NODE_ENV !== 'test') {
  const server = app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
  });

  const shutdown = (): void => {
    server.close(() => {
      database.close();
      process.exit(0);
    });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

export default app;
