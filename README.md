
# Metabase Driver: DB2

Works with DB2 11.5 for z/OS with no use of DB2_COMPATIBILITY_VECTOR and no DB2_DEFERRED_PREPARE_SEMANTICS. 

###  Running Metabase application with DB2 driver plugin
First download Metabase Jar File [here](https://metabase.com/start/other.html)  and run
```bash
java -jar metabase.jar
```
The `plugins/` directory will be created. Drop the driver in your `plugins/` directory. You can grab it [here](https://github.com/cabo40/metabase-db2-driver/releases/download/v0.1-alpha/db2.metabase-driver.jar) or build it yourself:

##  Editing the plugin: Prerequisites

### Java JDK 8
Check java, javac and jar installation
```bash
java -version
javac -version
jar
```

### Node.js
Install [Node.js]([https://nodejs.org](https://nodejs.org/))
```bash
sudo apt-get install curl
curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
sudo apt-get install nodejs
node -v 
```
### Leininger
Install [Leininger]([https://leiningen.org/](https://leiningen.org/))
```bash
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod +x lein
sudo mv lein /usr/local/bin
lein -v
```

### Yarn
Install [Yarn]([https://yarnpkg.com/lang/en/](https://yarnpkg.com/lang/en/))
```bash
curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
sudo apt-get update && sudo apt-get install yarn
yarn --version
```

## Editing the plugin: Building the driver 

### Clone the Metabase project

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

### Clone the DB2 Metabase Driver

Clone the [DB2 driver repo](https://github.com/alisonrafael/metabase-db2-driver) inside drivers modules folder `/metabase_source/modules/drivers`.

### Compile Metabase for building drivers
```bash
cd /path/to/metabase_source
lein install-for-building-drivers
```

### Compile the DB2 driver
```bash
./bin/build-driver.sh db2
```

### Copy it to your plugins dir
```bash
mkdir -p /path/to/metabase/plugins/
cp /metabase_source/modules/drivers/db2/target/uberjar/db2.metabase-driver.jar /path/to/metabase/plugins/
```

### Run Metabase

```bash
jar -jar /path/to/metabase/metabase.jar
```

## Configurations

You can run as follows to avoid the CharConversionException exceptions. By this way, JCC converts invalid characters to NULL instead of throwing exceptions.

```bash
java -Ddb2.jcc.charsetDecoderEncoder=3 -jar metabase.jar
```

or set it as an environment variable  

```bash
export JAVA_TOOL_OPTIONS="-Ddb2.jcc.charsetDecoderEncoder=3"
```

I recommend the following additional connection parameters for performance:

```bash
defaultIsolationLevel=1;
```
for uncommited read ("dirty" read), and

```bash
readOnly=true;
```


## Thanks
Thanks to everybody here [https://github.com/metabase/metabase/issues/1509](https://github.com/metabase/metabase/issues/1509)
