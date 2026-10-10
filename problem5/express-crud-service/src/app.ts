import express from 'express';
import { errorHandler } from './middleware/error-handler.js';
import { resourceRoutes } from './routes/resource-routes.js';

export const app = express();

app.use(express.json({ limit: '100kb' }));
app.get('/', (_request, response) => {
  response.json({ message: 'Hello from Express + TypeScript!' });
});
app.use('/api/resources', resourceRoutes);
app.use(errorHandler);

export default app;
