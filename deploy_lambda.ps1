# deploy_lambda.ps1 - PowerShell script to package and deploy Lambda function with dependencies

param(
    [string]$FunctionName = "youtube-playlist-builder",
    [string]$Region = "us-east-1",
    [string]$ZipFile = "lambda-deployment.zip",
    [string]$BuildDir = "build"
)

Write-Host "🚀 Starting Lambda deployment process..." -ForegroundColor Green

# Clean up previous builds
Write-Host "🧹 Cleaning up previous builds..." -ForegroundColor Yellow
if (Test-Path $BuildDir) { Remove-Item -Recurse -Force $BuildDir }
if (Test-Path $ZipFile) { Remove-Item -Force $ZipFile }

# Create build directory
New-Item -ItemType Directory -Path $BuildDir | Out-Null

# Install dependencies
Write-Host "📦 Installing Python dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt -t $BuildDir/

# Copy Lambda function code
Write-Host "📋 Copying Lambda function code..." -ForegroundColor Yellow
Copy-Item lambda.py $BuildDir/

# Create deployment package
Write-Host "📦 Creating deployment package..." -ForegroundColor Yellow
Set-Location $BuildDir
Compress-Archive -Path * -DestinationPath "../$ZipFile" -Force
Set-Location ..

$packageSize = (Get-Item $ZipFile).Length / 1MB
Write-Host "✅ Deployment package created: $ZipFile" -ForegroundColor Green
Write-Host "📊 Package size: $([math]::Round($packageSize, 2)) MB" -ForegroundColor Cyan

Write-Host "🎉 Deployment package ready!" -ForegroundColor Green
Write-Host "To deploy to AWS Lambda, run:" -ForegroundColor Yellow
Write-Host "aws lambda update-function-code --function-name $FunctionName --zip-file fileb://$ZipFile --region $Region" -ForegroundColor White

