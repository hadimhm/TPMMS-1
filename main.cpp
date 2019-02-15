//  main.cpp
//  Created by Sina Pilehchiha on 2019-02-07.
//  Copyright © 2019 T9. All rights reserved.
//  local includes
#include "kwaymergesort.h"
int main(int argc, char* argv[]) {
    std::string inFile = argv[1]; //    this argument is given to the executable pogram via the command line interface.
    std::string outFile = argv[2]; //    this argument is also given to the executable pogram via the command line interface.
    int  bufferSize = 2000;      // allow the sorter to use 2Kb (base 10) of memory for sorting.
    //  once full, it will dump to a temp file and grab another chunk.
    std::string tempPath     = "./";        //  allows you to write the intermediate files anywhere you want.
    KwayMergeSort *claim_sorter = new KwayMergeSort (inFile, outFile, bufferSize, tempPath); //  create a new instance of the KwayMergeSort class.
    const clock_t BEGINNING = clock(); //   mark the beginning of the execution of the sorting procedure
    claim_sorter->Sort(); //    this is the main call to the sorting procedure.
    const double EXECUTION_TIME = (double)(clock() - BEGINNING) / CLOCKS_PER_SEC / 60; //   report the execution time (in minutes)
    std::cout << EXECUTION_TIME << "\n"; // print out the time elapsed sorting.
}
