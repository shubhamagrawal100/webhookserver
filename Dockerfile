FROM node:14

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./
RUN npm install

# Bundle app source
COPY . .

# Bind the app to port 3000
EXPOSE 3000

# Start the app
CMD ["node", "server.js"]