# Stage 1: Build
FROM node:18-slim AS builder

# Set working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install all dependencies (including devDependencies)
RUN npm install

# Copy source code
COPY . .

# Build the TypeScript code
RUN npm run build

# Stage 2: Production
FROM node:18-slim

# Set working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install only production dependencies
RUN npm install --production

# Copy build artifacts from the builder stage
COPY --from=builder /usr/src/app/build ./build

# Expose the port
EXPOSE 8080

# Start the application
CMD [ "node", "build/server.js" ]
