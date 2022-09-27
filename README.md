# Grove


## Build

### Install Dependencies

* Install serd
    ```
    sudo apt-get install libserd-0-0 libserd-dev
    ```


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


### Compile Grove
Clone this repository and compile from source code

```
git clone
cd grove
make
sudo make install
```

## Usage
Create a database and load data

```
grovetool create watdiv100MDB
grovetool load watdiv100MDB ./watdiv100M.nt
```

Run SPARQL queries

```
grovetool query watdiv100MDB --input-file=/benchmark/watdiv/100M/S1.rq
```
