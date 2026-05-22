#!/bin/bash
mkdir -p secrets
cd secrets

# 1. Create the passwords for the Confluent Docker image to read
printf "confluent" > truststore_creds
printf "confluent" > keystore_creds
printf "confluent" > key_creds

# 2. Generate a Certificate Authority (CA)
echo "Generating CA..."
openssl req -new -newkey rsa:4096 -days 365 -x509 -subj "/CN=Kafka-CA" -keyout ca.key -out ca.crt -nodes

# 3. Create a Truststore (Brokers use this to verify each other and clients)
echo "Generating Truststore..."
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca.crt -storepass confluent -keypass confluent -noprompt

# 4. Loop through and create a Keystore for all 3 brokers
for i in 1 2 3; do
    echo "Generating Keystore for kafka$i..."
    # Create the Keystore
    keytool -keystore kafka$i.keystore.jks -alias localhost -validity 365 -genkey -keyalg RSA -dname "CN=localhost" -storepass confluent -keypass confluent
    
    # Create a Certificate Signing Request (CSR)
    keytool -keystore kafka$i.keystore.jks -alias localhost -certreq -file cert-file -storepass confluent
    
    # Sign the broker's certificate with our CA
    openssl x509 -req -CA ca.crt -CAkey ca.key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:confluent
    
    # Import the CA and the Signed Cert back into the broker's Keystore
    keytool -keystore kafka$i.keystore.jks -alias CARoot -import -file ca.crt -storepass confluent -noprompt
    keytool -keystore kafka$i.keystore.jks -alias localhost -import -file cert-signed -storepass confluent -noprompt
done

# Cleanup temp files
rm cert-file cert-signed ca.srl ca.key
echo "Certificates generated successfully in the /secrets folder!"
