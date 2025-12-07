#!/bin/bash
# Script de configuración e inicio de Docker para AquaSenseCloud API

# --- 1. INSTALACIÓN Y CONFIGURACIÓN DE DOCKER ---
yum update -y
yum -y install docker
systemctl start docker
systemctl enable docker
usermod -a -G docker ec2-user

# Esperar un momento para que Docker se inicialice (necesario en algunos entornos)
sleep 5

#   Variables
export AWS_DEFAULT_REGION="us-east-1"
export REGION=$AWS_DEFAULT_REGION
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET=$(aws s3api list-buckets \
  --query "Buckets[?starts_with(Name, 'proy-marmenor-codebucket-$ACCOUNT_ID')].Name" \
  --output text)

# Descarga la lambda y la comprimo:
wget https://raw.githubusercontent.com/franjavi-upct-es/Proyecto-ICAP-AquaSense/main/Ingesta%20Datos%20y%20Pipeline/funcion_lambda.py


zip lambda.zip funcion_lambda.py

# Subir la lambda al bucket
aws s3 cp lambda.zip s3://$BUCKET/lambda.zip

# Crear directorio de trabajo y moverse a el
mkdir -p /app
cd /app

# 2. Descargar requirements.txt desde GitHub
wget -O requirements.txt https://raw.githubusercontent.com/franjavi-upct-es/Proyecto-ICAP-AquaSense/refs/heads/main/Servidor%20Web%20y%20Containers/requirements.txt

# 3. Descargar Dockerfile desde GitHub
wget -O Dockerfile.txt https://raw.githubusercontent.com/franjavi-upct-es/Proyecto-ICAP-AquaSense/refs/heads/main/Servidor%20Web%20y%20Containers/Dockerfile.txt
mv Dockerfile.txt Dockerfile

# 4. Descargar aquasense.py desde GitHub
wget -O aquasense.py https://raw.githubusercontent.com/franjavi-upct-es/Proyecto-ICAP-AquaSense/refs/heads/main/Servidor%20Web%20y%20Containers/aquasense.py

# --- 5. CONSTRUCCIÓN DE LA IMAGEN DOCKER ---
docker build -t aquasense-api .

# --- 6. EJECUCIÓN DEL CONTENEDOR DOCKER ---
# Estas variables se inyectarán desde CloudFormation a la EC2, y luego 
# se pasan al contenedor.
DDB_TABLE_NAME="${DYNAMODB_TABLE_NAME}" 
AWS_REGION="${AWS_REGION}"             

# docker run -d \
#   --name aquasense-web \
#   -p 8080:8080 \
#   -e DYNAMODB_TABLE=$DDB_TABLE_NAME \
#   -e AWS_REGION=$AWS_REGION \
#   aquasense-api


aws ecr create-repository --repository-name aquasense-api

aws ecr get-login-password --region $REGION \
  | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

docker tag aquasense-api:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/aquasense-api:latest

docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/aquasense-api:latest
