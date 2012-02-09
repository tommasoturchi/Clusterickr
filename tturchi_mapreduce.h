    //
    //  tturchi_mapreduce.h
    //
    //  Created by Tommaso Turchi on 2/1/12.
    //  Copyright (c) 2012 Tommaso Turchi. All rights reserved.
    //
#ifndef clustering_tturchi_mapreduce_h
#define clustering_tturchi_mapreduce_h

#define SIZE 100000

#include <algorithm>
#include <iostream>
#include <list>
#include <omp.h>
#include <stdexcept>
#include <vector>

    // Key pairs format
template<typename T>
struct mr_k {
    unsigned int key;
    T* value;
    mr_k(): key(0), value(new T) {};
    mr_k(int key, int size): key(key), value(new T[size]) {};
    mr_k(int key, T* value): key(key), value(value) {};
    bool operator < (const mr_k& rhs) const { return key < rhs.key; }
};

    // Main function to control operations
template<typename T1,typename T2,typename T3>
int mr_shm(std::list<mr_k<T2> >* (*map)(mr_k<T1>*), T3* (*reduce)(mr_k<T2>*, unsigned int), unsigned int mappers, unsigned int reducers, mr_k<T1>* input, unsigned int ilen, T3* output)
{
        // Shuffling array used to sort and distribute data produced by mappers between reducers
    std::vector<mr_k<T2> >* shuffled = new std::vector<mr_k<T2> >(SIZE, *new mr_k<T2>(std::numeric_limits<unsigned int>::max(),0));
    int done = 0;
    std::list<mr_k<T2> >* current;
        // Mapping phase
#pragma omp parallel for num_threads(mappers) shared(ilen,input,map,mappers,shuffled) reduction(+:done) firstprivate(current) schedule(static)
    for (int i = 0; i < ilen; i++) {
        current = map(&input[i]);
        while ((int)current->size() > 0) {
            shuffled->at(omp_get_thread_num()*SIZE/mappers + done) = current->front();
            current->pop_front();
            done++;
                // Checking if shooted over boundaries
            if (done > SIZE/mappers) throw std::runtime_error("Shuffling vector size insufficient!!!");
        }
    }
    
        // Sorting the intermediate data
    std::sort(shuffled->begin(), shuffled->end());
        // Deleting unused elements and resizing
    shuffled->erase(shuffled->begin()+done+1, shuffled->end());
    shuffled->resize(done+1);
        // Preparing data to be reduced
    mr_k<T2>* buff;
    std::vector<int>* buffsizes = new std::vector<int>();
    for (int i = 0; i < done; i++)
        for (int j = i + 1; j <= done; j++) {
            if (shuffled->at(i) < shuffled->at(j)) {
                if (j > i + 1) {
                    buff = new mr_k<T2>(shuffled->at(i).key, j-i);
                    buffsizes->push_back(j-i);
                    for (int t = 0; t < j-i; t++) buff->value[t] = *shuffled->at(i+t).value;
                    shuffled->at(i) = *buff;
                    shuffled->erase(shuffled->begin()+i+1, shuffled->begin()+j);
                }
                done -= j - i - 1;
                break;
            }
        }
    output = new T3[done];
        // Reducing phase
#pragma omp parallel for num_threads(reducers) shared(buffsizes,output,reduce,shuffled) schedule(dynamic)
    for (int i = 0; i < done; i++) output[i] = *reduce(&shuffled->at(i), buffsizes->at(i));
        // Cleaning data
    buffsizes->clear();
    shuffled->clear();
    return done;
}
#endif