# One image, three roles: the compose file picks the entrypoint for
# price-producer, alert-engine and gateway.
FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src
COPY public ./public

USER node
EXPOSE 8080
CMD ["node", "src/gateway/index.js"]
