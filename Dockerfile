FROM node:20-slim

WORKDIR /app

# Install git so npm can resolve transitive git dependencies (like libsignal)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy package files first to leverage Docker build caching
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Copy the server source code
COPY server.js .
COPY nats_consumer.js .

# Expose the internal port Baileys is listening on
EXPOSE 3001

CMD ["node", "server.js"]
