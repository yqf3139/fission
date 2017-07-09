# A docker image for the func container.

FROM node:8.1.3

ARG NODE_ENV
ENV NODE_ENV $NODE_ENV

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json /usr/src/app/
RUN npm install

ADD server.js /usr/src/app/server.js
ADD service-catalog-clients /usr/src/app/service-catalog-clients

EXPOSE 8888
CMD [ "npm", "start" ]