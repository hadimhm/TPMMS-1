//  main.cpp
//  Created by Sina Pilehchiha on 2019-02-07.
//  Copyright © 2019 TeamNumber02. All rights reserved.

//  local includes
#include "Kwaymergesort.h"

//  a program shall contain a global function named main, which is the designated start of the program.
int main(int argc, char* argv[]) {
    
    //  this argument is given to the executable pogram via the command line interface.
    std::string inputFile = argv[1];
    
    //  this argument is also given to the executable pogram via the command line interface.
    std::string outputFile = argv[2];
    
    //  this argument is, too, given to the executable pogram via the command line interface.
    std::string sumOfCompensationAmountsFile = argv[3];
    
    //  allow the sorter to use an arbitrary amount (in bytes) of memory for sorting.
    std::string bufferSize = argv[4];
    
    //  once the buffer is full, the sorter will dump the buffer's content to a temporary file and grab another chunk from the input file.
    std::string temporaryPath = argv[5]; // allows you to write the intermediate files anywhere you want.
    
    //  create a new instance of the KwayMergeSort class.
    KwayMergeSort* claim_sorter = new KwayMergeSort (inputFile                      ,
                                                     outputFile                     ,
                                                     sumOfCompensationAmountsFile,
                                                     bufferSize                  ,
                                                     temporaryPath);
    
    //  mark the beginning of the execution of the sorting procedure
    const clock_t BEGINNING = clock();
    
    //  this is the main call to the sorting procedure.
    claim_sorter->Sort();
    
    //  report the execution time (in minutes)
    const double EXECUTION_TIME = (double)(clock() - BEGINNING) / CLOCKS_PER_SEC / 60;
    
    //  print out the time elapsed sorting.
    std::cout << EXECUTION_TIME << "\n";
}
