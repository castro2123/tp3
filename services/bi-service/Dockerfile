FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY services/bi-service ./bi-service

EXPOSE 3000

CMD ["node", "bi-service/server.js"]
