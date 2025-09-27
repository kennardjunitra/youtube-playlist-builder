#!/bin/bash
# deploy_lambda.sh - Script to package and deploy Lambda function with dependencies

set -e

# Configuration
FUNCTION_NAME="youtube-playlist-builder"
REGION="us-east-1"  # Change this to your preferred region
ZIP_FILE="lambda-deployment.zip"
BUILD_DIR="build"

echo "🚀 Starting Lambda deployment process..."

# Clean up previous builds
echo "🧹 Cleaning up previous builds..."
rm -rf $BUILD_DIR
rm -f $ZIP_FILE

# Create build directory
mkdir -p $BUILD_DIR

# Install dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt -t $BUILD_DIR/

# Copy Lambda function code
echo "📋 Copying Lambda function code..."
cp lambda.py $BUILD_DIR/

# Create deployment package
echo "📦 Creating deployment package..."
cd $BUILD_DIR
zip -r ../$ZIP_FILE .
cd ..

echo "✅ Deployment package created: $ZIP_FILE"
echo "📊 Package size: $(du -h $ZIP_FILE | cut -f1)"

# Deploy to AWS Lambda (uncomment and modify as needed)
# echo "🚀 Deploying to AWS Lambda..."
# aws lambda update-function-code \
#     --function-name $FUNCTION_NAME \
#     --zip-file fileb://$ZIP_FILE \
#     --region $REGION

echo "🎉 Deployment package ready!"
echo "To deploy to AWS Lambda, run:"
echo "aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://$ZIP_FILE --region $REGION"

