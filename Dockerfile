FROM maven:3-eclipse-temurin-11-alpine
MAINTAINER mike@rhodey.org

RUN apk add --no-cache nodejs npm curl jq

RUN mkdir -p /app/pipe
WORKDIR /app/pipe

COPY pom.xml pom.xml
RUN mvn verify

COPY package.json package.json
COPY package-lock.json package-lock.json
RUN npm install

COPY src/ src/
RUN mvn package

COPY index.js index.js
COPY tests.js tests.js
