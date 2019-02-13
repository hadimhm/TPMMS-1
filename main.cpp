//  main.cpp
//  Created by Sina Pilehchiha on 2019-02-07.
//  Copyright © 2019 T9. All rights reserved.
//  local includes
#include "kwaymergesort.h"
int main(int argc, char* argv[]) {
    std::string inFile = argv[1]; //    this argument is given to the executable pogram via the command line interface.
    int  bufferSize = 2000;      // allow the sorter to use 2Kb (base 10) of memory for sorting.
    //  once full, it will dump to a temp file and grab another chunk.
    bool compressOutput = false;       //   not yet supported
    std::string tempPath     = "./";        //  allows you to write the intermediate files anywhere you want.
    KwayMergeSort *claim_sorter = new KwayMergeSort (inFile, &std::cout, bufferSize, compressOutput, tempPath);
    claim_sorter->Sort();
}
