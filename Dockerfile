# Apify's official Playwright + Node 20 base image
FROM apify/actor-node-playwright-chrome:20

# Copy source
COPY package*.json ./
RUN npm --quiet set progress=false && npm install --omit=dev --omit=optional

COPY . ./

CMD npm start
