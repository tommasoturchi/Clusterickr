    //
    //  tturchi_clustering.cpp
    //
    //  Created by Tommaso Turchi on 2/1/12.
    //  Copyright (c) 2012 Tommaso Turchi. All rights reserved.
    //
#define MAPPERS 2
#define REDUCERS 1
#define EPSILON 0.000001

#include <fstream>
#include <iostream>
#include <math.h>
#include <sstream>
#include <sys/time.h>
#include "tturchi_mapreduce.h"
#include <vector>

        // Data format used by the algorithm
        // World Geodetic System standard coordinates combined with Flickr photo URL
struct wgs {
    double north;
    double east;
    char url[80];
    wgs(): north(0), east(0) {};
    wgs(double n, double e): north(n), east(e) {};
    wgs(double n, double e, char* u): north(n), east(e) { std::strcpy(url, u); };
};

        // Vector to store current centroids
static std::vector<wgs>* roids;

        // Function to compute Euclidean distance
static double eucd(wgs* p, wgs* q)
{
    return sqrtf(pow(p->north - q->north, 2) + pow(p->east - q->east, 2));
}

        // Map function
        // Changes the pair's key to the nearest centroid's id
static std::list<mr_k<wgs> >* map(mr_k<wgs>* in)
{
    for (int i = 0; i < roids->size(); i++) if (eucd(in->value, &roids->at(i)) < eucd(in->value, &roids->at(in->key))) in->key = i;
    return new std::list<mr_k<wgs> >(1, *in);
}

        // Reduce function
        // Compute the average centroid's coordinates associated to a group of points
static wgs* reduce(mr_k<wgs>* values, unsigned int len)
{
    roids->at(values->key).north = 0;
    roids->at(values->key).east = 0;
    for (int i = 0; i < len; i++) {
        roids->at(values->key).north += values->value[i].north/len;
        roids->at(values->key).east += values->value[i].east/len;
    }
    return &roids->at(values->key);
}

        // Main function
int main (int argc, const char * argv[])
{
#ifdef CHECKS
    if (argc != 4) {
        std::cout << "Missing argument(s), exiting..." << std::endl;
		exit (1);
    }
#endif
        // file to read
	std::ifstream input(argv[2]);
#ifdef CHECKS
	if (!input) {
        std::cout << "File doesn't exists, exiting..." << std::endl;
		exit (1);
    }
#endif
        // file to write
    std::ofstream output(argv[3]);
        // # of clusters
    int clus = atoi(argv[1]);
        // input array
    std::vector<mr_k<wgs> >* data = new std::vector<mr_k<wgs> >();
        // output array
    wgs* out;
        // read data into array
    int ilen = 0;
    double north, east;
    char buff[100];
    std::stringstream ss;
    while(!input.eof()) {
        input.getline(buff, 100);
        ss << buff;
        ss.getline(buff, 10, ' ');
        north = atof(buff);
        ss.getline(buff, 10, ' ');
        east = atof(buff);
        ss.getline(buff, 80);
        ss << "";
        ss.clear();
        data->push_back(*new mr_k<wgs>(0,new wgs(north, east, buff)));
        ilen++;
    }
    input.close();
        // centroids array
    roids = new std::vector<wgs >(clus,*new wgs());
        // select centroids between input points randomly
    timeval t;
    int r;
    for (int i = 0; i < clus; i++) {
        gettimeofday(&t, NULL);
        std::srand((unsigned int)(t.tv_usec*t.tv_sec));
        r = rand()%ilen;
        roids->at(i).north = data->at(r).value->north;
        srand((unsigned int)time(NULL));
        roids->at(i).east = data->at(r).value->east;
    }
    float eps = std::numeric_limits<float>::max();
    std::cout << "Starting..." << std::endl;
    int done;
    timeval start;
    gettimeofday(&start, NULL);
    while (eps > EPSILON) {
        eps = 0;
            // copy centroids values so they will be compared with new values to check the while guard
        std::vector<wgs>* oldroids = new std::vector<wgs>(roids->begin(), roids->end());
            // do the magic
        try {
            done = mr_shm(*map, *reduce, MAPPERS, REDUCERS, data->data(), ilen, out);
        } catch (std::runtime_error ex) {
            std::cout << ex.what() << std::endl;
        }
            // compute the distance from the old centroids
        for (int i = 0; i < clus; i++) eps += eucd(&roids->at(i), &oldroids->at(i));
    }
        // write results
    for (int i = 0; i < (int)data->size(); i++) output << data->at(i).key << std::endl;
    output.close();
        // print final centroids coordinates
    for (int i = 0; i < done; i++) std::cout << "Centroid " << i << " : (" << roids->at(i).east << "," << roids->at(i).north << ")" << std::endl;
    timeval end;
    gettimeofday(&end, NULL);
    double elapsed = end.tv_sec+end.tv_usec/1000000.0 - start.tv_sec-start.tv_usec/1000000.0;
    std::cout << "elapsed time : "<< elapsed << " secs." << std::endl;
    return 0;
}

