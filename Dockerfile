# Use the official Node.js 14.17.3 image as the base image
FROM node:18.20.1
# Set the working directory inside the container
RUN mkdir /adres
WORKDIR /adres
# Copy package.json and package-lock.json to the working directory
ADD package*.json ./
# Install dependencies
RUN npm install
RUN npm install dotenv
# Copy the application code to the working directory
ADD . /adres/