//
//  main.cpp
//  LA1++
//
//  Created by Sina Pilehchiha on 2019-02-07.
//  Copyright © 2019 T9. All rights reserved.

#include <cstdlib>
#include <math.h>
using namespace std;

// local includes
#include "kwaymergesort.h"
// a basic struct for a BED entry.
struct BED {
    char clientID[10];
    char compensationAmount[10];
    //int clientID;
    //float compensationAmount;
    
    bool operator < (const BED &b) const
    {
        
        if      (atoi(clientID) < atoi(b.clientID))  return true;
        else if (atoi(clientID) > atoi(b.clientID))  return false;
        // we get here when chroms are the same. now sort on starts
        if      (atof(compensationAmount) < atof(b.compensationAmount))  return true;
        else return false;
         
        /*if      (clientID < b.clientID)  return true;
        else if (clientID > b.clientID)  return false;
        // we get here when chroms are the same. now sort on starts
        if      (compensationAmount < b.compensationAmount)  return true;
        else return false;*/
    }
    
    // overload the << operator for writing a BED struct
    friend ostream& operator<<(ostream &os, const BED &b)
    {
        
        os << "                  " << b.clientID << "                                                                                                                                                                                                                      " << b.compensationAmount;
        /*os << b.clientID << "\t" << b.compensationAmount << "\n";*/
        return os;
    }
    // overload the >> operator for reading into a BED struct
    friend istream& operator>>(istream &is, BED &b)
    {
        is.ignore(18);
        is.get(b.clientID, 10);
        is.ignore(214);
        is.get(b.compensationAmount, 10);
        is.ignore(1);
        return is;
    }
};
// comparison function for sorting by chromosome, then by start.
bool bySize(BED const &a, BED const &b) {
    if (atoi(a.clientID) != atoi(b.clientID)) return (atoi(a.clientID) < atoi(b.clientID));
    else return (atof(a.compensationAmount) < atof(b.compensationAmount));
    /*if (a.clientID != b.clientID) return (a.clientID < b.clientID);
    else return (a.compensationAmount < b.compensationAmount);*/
}


int main(int argc, char* argv[]) {
    
    string inFile       = argv[1];
    int  bufferSize     = 2000;      // allow the sorter to use 100Kb (base 10) of memory for sorting.
    // once full, it will dump to a temp file and grab another chunk.
    bool compressOutput = false;       // not yet supported
    string tempPath     = "./";        // allows you to write the intermediate files anywhere you want.
    
    // sort a BED file by chrom then start
    KwayMergeSort<BED> *bed_sorter = new KwayMergeSort<BED> (inFile,
                                                             &cout,
                                                             bufferSize,
                                                             compressOutput,
                                                             tempPath);
    bed_sorter->SetComparison(bySize);
    bed_sorter->Sort();
}
