How to generate JNI
---

References:
- https://www.baeldung.com/jni
- https://github.com/juddgaddie/jna-jni-time/tree/master/src/nativetime

`$ javac -h . Clock.java`

`$ mv com_blokaly_ceres_system_Clock.cpp nativetime.cpp`

`$ g++ -c -fPIC -O2 -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux nativetime.cpp -o nativetime.o`

`$ g++ -shared -fPIC -o libnativetime.so nativetime.o -lc`