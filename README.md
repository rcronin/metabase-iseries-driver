
# Metabase Driver: DB2

###  Running Metabase application with DB2 driver plugin
First download Metabase Jar File [here](https://metabase.com/start/other.html)  and run
```bash
java -jar metabase.jar
```
The `plugins/` directory will be created. Drop the driver in your `plugins/` directory. You can grab it [here](https://github.com/alisonrafael/metabase-db2-driver/target/uberjar/db2.metabase-driver.jar) or build it yourself:

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

Clone the [DB2 driver repo](https://github.com/alisonrafael/metabase-db2-driver) inside drivers modules folder `/metabase_source/modules/drivers`

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

## ISSUES
* Filter between dates in "custom question" do not work.
DB error:
`..."DB2 SQL Error: SQLCODE=-245, SQLSTATE=428F5, SQLERRMC=DATE"...`
Metabase params:
`...:query {:source-table 24029, :filter ["and" ["between" ["field-id" 142431] "2018-09-18" "2019-09-18"]]...`
Generated SQL:
`...WHERE date(\"SCHEMA\".\"TABLE\".\"DT_COLUMN\") BETWEEN date(?) AND date... :params (#inst "2018-09-18T03:00:00.000000000-00:00" #inst "2019-09-18T03:00:00.000000000-00:00")...`

## Thanks
Thanks to everybody here [https://github.com/metabase/metabase/issues/1509](https://github.com/metabase/metabase/issues/1509)