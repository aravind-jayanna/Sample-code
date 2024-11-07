# S3 CSV Updation Project

This project demonstrates how to interact with an Amazon S3 bucket to read, update, and upload CSV files using Node.js. The project leverages the `aws-sdk` for S3 operations, `fast-csv` for reading and writing CSV files, and JWT-based authentication for securing the operations.

## Table of Contents
- [Overview](#overview)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Environment Configuration](#environment-configuration)
- [Usage](#usage)
- [License](#license)

## Overview

This project allows you to:
- Read a CSV file from an S3 bucket.
- Modify the CSV data (e.g., delete specific rows based on an ID).
- Write the updated CSV data back to the S3 bucket.

Additionally, JWT authentication is used for secure access to the endpoints that modify the S3 bucket contents.

## Getting Started

### Prerequisites

Before starting, ensure you have the following tools installed:
- [Node.js](https://nodejs.org/) (v14 or higher recommended)
- [AWS Account](https://aws.amazon.com/) with S3 bucket access
- A `.env` file with appropriate AWS credentials and bucket details.
