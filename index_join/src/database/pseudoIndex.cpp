//
// Created by Scott on 2019-08-20.
// Alright reserved by Qinghzi Ma
// Email: Q.Ma.2@warwick.ac.uk
// Department of Computer Science, University of Warwick,  UK
//

#include "pseudoIndex.h"

PseudoIndexBuilder::PseudoIndexBuilder() {this->sampleSize = 100;}
PseudoIndexBuilder::PseudoIndexBuilder(int sampleSize) {this->sampleSize=sampleSize;}

int PseudoIndexBuilder::AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber) {
    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(LHSIndex);
    m_RHSJoinIndex.push_back(RHSIndex);

    return int(m_joinedTables.size());
}

void PseudoIndexBuilder::Build() {};
std::string PseudoIndexBuilder::Sample() { return "";};

