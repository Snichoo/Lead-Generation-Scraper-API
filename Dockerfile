# Use the official Node.js 18 image
FROM node:18-slim

# Set working directory
WORKDIR /usr/src/app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install --production

# Copy source code
COPY . .

# Build the TypeScript code
RUN npm install -g typescript
RUN npm run build

# Expose the port
EXPOSE 8080

# Start the application
CMD [ "node", "build/server.js" ]
