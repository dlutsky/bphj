# BPHJ


## Build

### Install Dependencies


* Install libdb++-dev
    ```
    sudo apt-get install libdb++-dev
    ```

* Install yaml-cpp
    ```
    sudo apt-get install libyaml-cpp-dev
    ```

* Install rocksdb
    ```
    git clone https://github.com/facebook/rocksdb.git
    cd rocksdb
    make static_lib
    sudo make install-static
    ```

* Install croaring
    ```
    git clone https://github.com/RoaringBitmap/CRoaring.git
    cd CRoaring
    mkdir -p build
    cd build
    cmake ..
    sudo make install
    ```


### Compile BPHJ
Clone this repository and compile from source code

```
git clone
cd bphj
make
sudo make install
```

## Usage
Create a database and load data

```
dbtool create watdiv100MDB
dbtool load watdiv100MDB ./watdiv100M.nt
```

Run SPARQL queries

```
dbtool query watdiv100MDB --input-file=/benchmark/watdiv/100M/S1.rq
```
