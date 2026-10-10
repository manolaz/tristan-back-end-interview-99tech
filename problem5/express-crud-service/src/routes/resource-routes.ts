import { Router } from 'express';
import { resourceController } from '../controllers/resource-controller.js';

export const resourceRoutes = Router();

resourceRoutes.post('/', resourceController.create);
resourceRoutes.get('/', resourceController.list);
resourceRoutes.get('/:id', resourceController.get);
resourceRoutes.put('/:id', resourceController.replace);
resourceRoutes.patch('/:id', resourceController.update);
resourceRoutes.delete('/:id', resourceController.delete);
