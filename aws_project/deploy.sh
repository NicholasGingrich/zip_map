#!/bin/bash
set -e

# -----------------------------
# CONFIG — EDIT THESE
# -----------------------------
AWS_REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

REPO_NAME="zip-map-processor"
LAMBDA_NAME="zip-map-processor"

IMAGE_TAG="latest"

# -----------------------------
# Derived values
# -----------------------------
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}"
FULL_IMAGE="${ECR_URI}:${IMAGE_TAG}"

echo "Using image: $FULL_IMAGE"

# -----------------------------
# 1️⃣ Login to ECR
# -----------------------------
echo "Logging into ECR..."
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin $ECR_URI

# -----------------------------
# 2️⃣ Build image (force correct arch)
# -----------------------------
echo "Building Docker image..."
docker build --platform linux/amd64 -t $REPO_NAME .

# -----------------------------
# 3️⃣ Tag image for ECR
# -----------------------------
echo "Tagging image..."
docker tag $REPO_NAME:latest $FULL_IMAGE

# -----------------------------
# 4️⃣ Push to ECR
# -----------------------------
echo "Pushing image to ECR..."
docker push $FULL_IMAGE

# -----------------------------
# 5️⃣ Update Lambda to use new image
# -----------------------------
echo "Updating Lambda function..."

aws lambda update-function-code \
  --function-name $LAMBDA_NAME \
  --image-uri $FULL_IMAGE

echo "Waiting for update to complete..."

aws lambda wait function-updated \
  --function-name $LAMBDA_NAME

echo "✅ Deployment complete!"
