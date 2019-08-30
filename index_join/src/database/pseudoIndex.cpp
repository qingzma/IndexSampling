//
// Created by Scott on 2019-08-20.
// Alright reserved by Qinghzi Ma
// Email: Q.Ma.2@warwick.ac.uk
// Department of Computer Science, University of Warwick,  UK
//

#include "pseudoIndex.h"
#include "../util/StringSplitter.h"
#include "../util/Timer.h"

#include <iostream>
#include <regex>
#include <fstream>


PseudoIndexBuilder::PseudoIndexBuilder() {
    m_join_counts={};
    m_join_counts_acc={};
    m_sampleIndexContainer={};
}

int PseudoIndexBuilder::AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber) {
    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(LHSIndex);
    m_RHSJoinIndex.push_back(RHSIndex);

    return int(m_joinedTables.size());
}

void PseudoIndexBuilder::Build(){
    n_tables = m_LHSJoinIndex.size();

    if (m_RHSJoinIndex.at(0) != -1 || m_LHSJoinIndex.at(n_tables-1) != -1){
        std::cout<<m_LHSJoinIndex.at(0)<<m_RHSJoinIndex.at(n_tables-1)<<std::endl;
        throw std::runtime_error("The join tables are not in the correct order!");
    }



    auto table0= m_joinedTables.begin();
    std::shared_ptr < key_index > table0Index = table0->get()->get_key_index(m_LHSJoinIndex.at(0));

    // initialize m_join_counts
    for (key_index::iterator it = table0Index->begin(); it!= table0Index->end(); it++){
        try {
            m_join_counts.at("$_"+std::to_string(it->first)) += 1;
        } catch (std::out_of_range e){
            m_join_counts.emplace("$_"+std::to_string(it->first), 1);
        }
    }

//    for (auto item: m_join_counts){
//        std::cout<<item.first<<", "<<item.second<<std::endl;
//    }

    // build path for intermediate by-directional tables.
    for (int table_count=1; table_count<n_tables-1;table_count++){
//        std::cout<<"table count is "<< table_count<<std::endl;
//        std::shared_ptr< key_index > tableIIndex = m_joinedTables.at(table_count)->get_key_index(m_RHSJoinIndex.at(table_count));
        std::shared_ptr< composite_key_index > tableIComplexIndex = m_joinedTables.at(table_count)->get_composite_key_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
        std::shared_ptr< join_attributes_relation_index> tableIJoinIndex = m_joinedTables.at(table_count)->get_join_attribute_relation_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));

        std::map<std::string, int64_t> new_join_counts{};
        // iterate over m_join_counts
        for (auto item:m_join_counts){
//            std::cout<<"in item"<<std::endl;
            int64_t LTableLKey = StringSplitter::split_last_int64(item.first, "_");

            // if there is no join pair in the right table, delete the records in m_join_counts

            std::set<int64_t > RTableRKeys = tableIJoinIndex->at(LTableLKey);

            for (int64_t RTableRkey: RTableRKeys){
//                    std::cout<<"right label key is "<<RTableRkey<<std::endl;
                std::tuple<jfkey_t,jfkey_t> complex_key {LTableLKey, RTableRkey};
                new_join_counts[item.first+"-$_"+std::to_string(RTableRkey)]= m_join_counts[item.first] * tableIComplexIndex->count(complex_key);
            }
        }

        m_join_counts = new_join_counts;

//        for (auto item: m_join_counts){
//            std::cout<<item.first<<","<<item.second<<std::endl;
//        }
    }

    // build path for the right most table.
    std::map<std::string, int64_t> new_join_counts{};
    std::shared_ptr< key_index > tableIIndex = m_joinedTables.at(n_tables-1)->get_key_index(m_RHSJoinIndex.at(n_tables-1));
    for (auto item: m_join_counts){
        int64_t LTableLKey = StringSplitter::split_last_int64(item.first, "_");
        if (tableIIndex->count(LTableLKey) != 0){
            new_join_counts[item.first+"-$"]= m_join_counts[item.first] * tableIIndex->count(LTableLKey);
        }
    }

    m_join_counts = new_join_counts;

    m_cadinality = 0;
    m_join_counts_acc[0] = std::pair<std::string, int64_t>{"non_index", m_cadinality};

    int index = 1;
    for (auto item: m_join_counts){
        /* std::cout<<item.first<<","<<item.second<<std::endl;*/
        m_cadinality += item.second;
        m_join_counts_acc[index] = std::pair<std::string, int64_t>{item.first, m_cadinality};
        index ++;
    }

    /*for (auto value:m_join_counts_acc){
        std::cout<<value.first<<","<<value.second<<std::endl;
    }*/
};

int64_t PseudoIndexBuilder::getCardinality() {
    return m_cadinality;
}

void PseudoIndexBuilder::Sample(int sampleSize) {
    /* create samples without replacement*/
    std::set<int64_t > numbers{};
    /* Seed */
    std::random_device rd;
    /* Random number generator */
    std::default_random_engine generator(rd());
    /* Distribution on which to apply the generator */
    std::uniform_int_distribution<int64_t > distribution(1,m_cadinality);
    Timer timer;
    timer.reset();
    timer.start();
    /* fill the set with random number. */
    for( int i=0;i <sampleSize;i++){
        numbers.insert(distribution(generator));
    }
    /* make sure the sample size is as requested.*/
    while(numbers.size()<sampleSize){
        numbers.insert(distribution(generator));
    }

    for (int64_t number:numbers){
        JoinIndexItem joinIndexItem = getJoinIndexItem(number);

        if (m_sampleIndexContainer.count(joinIndexItem.tag) ==0){
            std::vector<JoinIndexItem> item {joinIndexItem};
            m_sampleIndexContainer[joinIndexItem.tag] = item;
        }
        else{
            m_sampleIndexContainer[joinIndexItem.tag].push_back(joinIndexItem);
        }
    }

    timer.stop();
    std::cout<<"it takes "<<timer.getMilliseconds()<<" milliseconds to create the random numbers, please remove them from the reported time cost."<<std::endl;

    fetchJoinTuples("pseudoSample.txt");

};

void PseudoIndexBuilder::Sample(int sampleSize, JoinOutputColumnContainer joinOutputColumnContainer) {

    Timer timer;
    timer.reset();
    timer.start();

    /* create samples without replacement*/
    std::set<int64_t > numbers{};
    /* Seed */
    std::random_device rd;
    /* Random number generator */
    std::default_random_engine generator(rd());
    /* Distribution on which to apply the generator */
    std::uniform_int_distribution<int64_t > distribution(1,m_cadinality);
    /* fill the set with random number. */
    for( int i=0;i <sampleSize;i++){
        numbers.insert(distribution(generator));
    }
    /* make sure the sample size is as requested.*/
    while(numbers.size()<sampleSize){
        numbers.insert(distribution(generator));
    }

    for (int64_t number:numbers){
        JoinIndexItem joinIndexItem = getJoinIndexItem(number);

        if (m_sampleIndexContainer.count(joinIndexItem.tag) ==0){
            std::vector<JoinIndexItem> item {joinIndexItem};
            m_sampleIndexContainer[joinIndexItem.tag] = item;
        }
        else{
            m_sampleIndexContainer[joinIndexItem.tag].push_back(joinIndexItem);
        }
    }

    timer.stop();
    std::cout<<"it takes "<<timer.getMilliseconds()<<" milliseconds to create the random numbers, please remove them from the reported time cost."<<std::endl;

    fetchJoinTuples("pseudoSample.txt",joinOutputColumnContainer);
}


PseudoIndexBuilder::JoinIndexItem PseudoIndexBuilder::getJoinIndexItem(int64_t num) {
    int l = 0;
    int r = m_join_counts_acc.size()-1;

    int targetIndex;

    // iterative Binary Search
    while (l <= r) {
        int m = l + (r - l) / 2;

        // Check if num is present at mid
        if (std::get<1>(m_join_counts_acc[m]) >= num &&
                std::get<1>(m_join_counts_acc[m-1]) <    num){
            targetIndex = m;
            /*std::cout<<"target index is  "<<targetIndex<<std::endl;*/
            break;
        }


        // If num greater, ignore left half
        if (std::get<1>(m_join_counts_acc[m]) < num){
            l = m + 1;
            /*std::cout<<"left is changed to "<<l<<std::endl;*/
        }
            // If num is smaller, ignore right half
        else{
            r = m - 1 ;
            /*std::cout<<"right is changed to "<<r<<std::endl;*/
        }

    }
    /* std::cout<<targetIndex<<", "<<std::get<0>(m_join_counts_acc[targetIndex])<<", "<<std::get<1>(m_join_counts_acc[targetIndex])<<std::endl;*/

    std::string comIndex =  std::get<0>(m_join_counts_acc[targetIndex]);
    /*std::cout<<comIndex<<std::endl;*/
    comIndex = std::regex_replace(comIndex, std::regex("-"), "");
    comIndex = std::regex_replace(comIndex, std::regex("_"), "");
    /*std::cout<<comIndex<<std::endl;*/

    std::vector<std::string> splitter = StringSplitter::split(comIndex,"\\$");

    JoinIndexItem joinIndexItem {};
    joinIndexItem.tag = targetIndex;    /*std::get<0>(m_join_counts_acc[targetIndex]);*/
    joinIndexItem.offsetNum = std::get<1>(m_join_counts_acc[targetIndex]) - num;
    /*std::cout<<comIndex<<std::endl;*/
    joinIndexItem.leftKey = std::stoi(splitter.at(0));
    joinIndexItem.rightKey = std::stoi(splitter.at(splitter.size()-1));
    joinIndexItem.pseudoIndex = num;

    std::vector<std::tuple<int64_t , int64_t >> midKeys{};
    for (int i =0;i < splitter.size()-1; i++){
        midKeys.push_back(std::make_tuple(std::stoi(splitter.at(i)),std::stoi(splitter.at(i+1))));
    }
    joinIndexItem.midKeys = midKeys;

    /*std::cout<<joinIndexItem.leftKey<<std::endl;
    for (auto value:midKeys){
        std::cout<<std::get<0>(value)<<","<<std::get<1>(value)<<std::endl;
    }
    std::cout<<joinIndexItem.rightKey<<std::endl;*/


    getJoinIndexItemIndexes(joinIndexItem);

    return joinIndexItem;
}

JoinIndexItemIndexes PseudoIndexBuilder::getJoinIndexItemIndexes(PseudoIndexBuilder::JoinIndexItem joinIndexItem) {

    std::map<int, int64_t > counts, reverse_acc_counts; /* counts of tuple in each group, [2,3,4,5], reverse_acc_counts [120,60,20,5]*/
    std::map<int, int64_t > index_positions; /* for a location of 4, the corresponding index_position is [0,0,0,4] */
    JoinIndexItemIndexes joinIndexItemIndexes;

    auto table0= m_joinedTables.begin();
    std::shared_ptr < key_index > table0Index = table0->get()->get_key_index(m_LHSJoinIndex.at(0));
    counts[0] = table0Index->count(joinIndexItem.leftKey);


    for (int table_count=1; table_count<n_tables-1;table_count++){
        std::shared_ptr< composite_key_index > tableICompositeIndex = m_joinedTables.at(table_count)->get_composite_key_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
        /*std::shared_ptr< join_attributes_relation_index> tableIJoinIndex = m_joinedTables.at(table_count)->get_join_attribute_relation_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));*/
        std::tuple<int64_t , int64_t > composite_key = joinIndexItem.midKeys.at(table_count-1);
        counts[table_count] = tableICompositeIndex->count(std::tuple<jfkey_t,jfkey_t>{std::get<0>(composite_key),std::get<1>(composite_key)});
        /*std::cout<<counts[table_count]<<std::endl;*/
    }

    std::shared_ptr< key_index > tableRIndex = m_joinedTables.at(n_tables-1)->get_key_index(m_RHSJoinIndex.at(n_tables-1));
    counts[n_tables-1] = tableRIndex->count(joinIndexItem.rightKey);


    /*counts ={};
    counts.emplace(0,2);
    counts.emplace(1,3);
    counts.emplace(2,4);
    counts.emplace(3,5);*/


    int64_t total_count=1;
    for (auto value: counts){
        total_count =  total_count*value.second;
        /*std::cout<<value.second<<", ";*/
    }
    /*std::cout<<std::endl;*/

    int64_t position = total_count - joinIndexItem.offsetNum;
    /*position = 75;*/


    reverse_acc_counts[n_tables-1 ] = counts[n_tables-1];
    for (int i = n_tables -2; i>=0;i--){
        reverse_acc_counts[i] = reverse_acc_counts[i+1]*counts[i];
    }

    /*for (auto value: reverse_acc_counts){
        std::cout<<value.second<<"- ";
    }
    std::cout<<std::endl;*/

    for(int i =0; i < n_tables-1; i++){
        index_positions[i] = position / reverse_acc_counts[i+1];
        position = position % reverse_acc_counts[i+1];
    }
    index_positions[n_tables - 1] = position % reverse_acc_counts[n_tables - 1];


    /*std::cout<<"position is "<<position<<std::endl;
    for (auto value: index_positions){
        std::cout<<value.second<<"... ";
    }
    std::cout<<std::endl;*/



    /* deal with the left most table */

    auto rangeIter = table0Index->equal_range(joinIndexItem.leftKey);
    if ( index_positions[0]==0){
        joinIndexItemIndexes[0]=rangeIter.first->second;

    }
    else{
        int64_t i=0;
        auto it = rangeIter.first;
        for (auto it = rangeIter.first; it != rangeIter.second; it++ ){
            if (i < index_positions[0]){
                i++;
                /*std::cout<<"try to locate"<<std::endl;*/
            }
        }
        joinIndexItemIndexes[0]= it->second;
    }

    /*std::cout<<"0 is "<<joinIndexItemIndexes[0]<<std::endl;*/





    /* deal with the intermediate tables.*/
    for (int table_count=1; table_count<n_tables-1;table_count++){
        std::shared_ptr< composite_key_index > tableICompositeIndex = m_joinedTables.at(table_count)->get_composite_key_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
        std::tuple<jfkey_t ,jfkey_t> key = joinIndexItem.midKeys.at(table_count-1);
        auto ranges  = tableICompositeIndex->equal_range(key);

        /* deal with one table per time*/
        if ( index_positions[table_count]==0){
            joinIndexItemIndexes[table_count]=ranges.first->second;

        }
        else{
            int64_t i=0;
            auto it = ranges.first;
            for (auto it = ranges.first; it != ranges.second; it++ ){
                if (i < index_positions[table_count]){
                    i++;
                    /*std::cout<<"try to locate"<<std::endl;*/
                }
            }
            joinIndexItemIndexes[table_count]= it->second;
        }

        /*std::shared_ptr< join_attributes_relation_index> tableIJoinIndex = m_joinedTables.at(table_count)->get_join_attribute_relation_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));*/
        /*counts[table_count] = tableICompositeIndex->count(std::tuple<jfkey_t,jfkey_t>{std::get<0>(composite_key),std::get<1>(composite_key)});*/
        /*std::cout<<counts[table_count]<<std::endl;*/
    }


    /* deal with the right most table*/
    rangeIter = tableRIndex->equal_range(joinIndexItem.rightKey);
    if ( index_positions[n_tables-1]==0){
        joinIndexItemIndexes[n_tables-1]=rangeIter.first->second;

    }
    else{
        int64_t i=0;
        auto it = rangeIter.first;
        for (auto it = rangeIter.first; it != rangeIter.second; it++ ){
            if (i < index_positions[n_tables-1]){
                i++;
                /*std::cout<<"try to locate"<<std::endl;*/
            }
        }
        joinIndexItemIndexes[n_tables-1]= it->second;
    }

    /*std::cout<<"4th is "<<joinIndexItemIndexes[n_tables-1]<<std::endl;*/

//    for (auto value : joinIndexItemIndexes){
//        std::cout<<value.first<<", "<<value.second<<std::endl;
//    }
    return joinIndexItemIndexes;
}


void PseudoIndexBuilder::fetchJoinTuples(std::string outfile) {
    std::ofstream output_file(outfile, std::ios::out | std::ofstream::app);


    for (auto i:m_sampleIndexContainer){
        for (JoinIndexItem joinIndexItem:i.second) {
            output_file << fetchJoinTupleRandom(joinIndexItem) << '\n';
        }
    }
    output_file.close();
}

void PseudoIndexBuilder::fetchJoinTuples(std::string outfile, JoinOutputColumnContainer joinOutputColumnContainer) {
    std::ofstream output_file(outfile, std::ios::out | std::ofstream::app);

    Timer timer0, timer1;
    double t0=0, t1=0;
    for (auto i:m_sampleIndexContainer){
        for (JoinIndexItem joinIndexItem:i.second) {
            timer0.reset();
            timer0.start();
            JoinIndexItemIndexes index = getJoinIndexItemIndexes(joinIndexItem);
            timer0.stop();
            t0+=timer0.getMicroseconds();

            timer1.reset();
            timer1.start();
            output_file << fetchJoinTupleUsingIndex(index, joinOutputColumnContainer, joinIndexItem.pseudoIndex) << '\n';
            timer1.stop();
            t1+=timer1.getMicroseconds();

        }
    }
    std::cout<<"index locating time cost: "<<t0<<std::endl;
    std::cout<<"tuple locating time cost: "<<t1<<std::endl;
    output_file.close();
}


std::string PseudoIndexBuilder::fetchJoinTupleRandom(JoinIndexItem joinIndexItem) {
    std::string outStr= "";
    /*outStr += std::to_string(joinIndexItem.leftKey);*/

    outStr += "  " + std::to_string(std::get<0>(joinIndexItem.midKeys.at(0)));
    for (auto value:joinIndexItem.midKeys){
        outStr += "  " +std::to_string(std::get<1>(value));
    }

    return outStr;
}

std::string PseudoIndexBuilder::fetchJoinTupleUsingIndex(JoinIndexItemIndexes joinIndexItemIndexes, JoinOutputColumnContainer joinOutputColumnContainer) {
    std::string outStr="";
    for (auto joinOutput:*joinOutputColumnContainer.getContainer()){
        /*std::cout<<"int table "<<std::endl;*/
        for (int column: joinOutput.second){
            /*std::cout<<"    int table columns:"<<column<<std::endl;*/
            switch (m_joinedTables.at(joinOutput.first)->getColumnTypes(column)){
                case DATABASE_DATA_TYPES::INT64 :
                    outStr += std::to_string(m_joinedTables.at(joinOutput.first)->get_int64(joinIndexItemIndexes.at(joinOutput.first),column)) +",";
                    break;
                case DATABASE_DATA_TYPES::CHAR :
                    std::string strs=m_joinedTables.at(joinOutput.first)->get_char(joinIndexItemIndexes.at(joinOutput.first),column);
                    outStr += strs  + ',';
                    break;
                /*default:
                    outStr += std::to_string(  m_joinedTables.at(joinOutput.first)->getColumnTypes(column)) + "unknown, ";
                    break;*/
            }

        }
    }
    outStr.pop_back();
    return outStr;
}

std::string PseudoIndexBuilder::fetchJoinTupleUsingIndex(JoinIndexItemIndexes joinIndexItemIndexes,
                                                         JoinOutputColumnContainer joinOutputColumnContainer,
                                                         int64_t pseudoIndex) {
    return std::to_string(pseudoIndex)+","+fetchJoinTupleUsingIndex(joinIndexItemIndexes,joinOutputColumnContainer);
}

