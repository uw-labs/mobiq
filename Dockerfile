FROM node:12-alpine

RUN mkdir -p /opt/mobiq

WORKDIR /opt/mobiq

ADD package.json .
ADD package-lock.json .

RUN npm i

ADD . .

ENTRYPOINT ["/opt/mobiq/bin/mobiq"]
