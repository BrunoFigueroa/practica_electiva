# Dockerfile para centralized-mape
FROM node:20

# Crear directorio de la app
WORKDIR /app

# Copiar package.json y package-lock.json si existe
COPY package*.json ./

# Instalar dependencias
RUN npm install

# Copiar el código fuente y el JAR
COPY . .

# Comando por defecto al iniciar el contenedor
CMD ["node", "mape.js"]
