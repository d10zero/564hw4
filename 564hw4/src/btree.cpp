/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <string.h>


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	//first, check if the index file exists
	//if the file exists, open it
	//if not, create a new file
	//need to do somethi9ngn different for each 3 types of int, double and string
	std::cout << "input parameters\n";
	std::cout << "relationName: " << relationName << "\n";
	std::cout << "outIndexName: " << outIndexName << "\n";
	std::cout << "bufMgrIn: " << bufMgrIn << "\n";
	std::cout << "attrByteOffset: " << attrByteOffset << "\n";
	std::cout << "attrType: " << attrType << "\n";
	bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	attributeType = attrType;

	//do the following depending on the attribute type of string,
	//int or double
	//**********from the project spec*********
	std::ostringstream idxStr;
	idxStr<<relationName<<'.'<<attrByteOffset;
	std::string indexName = idxStr.str();// indexName is the name of the index file

	if (attributeType == 0){
		//0 = interger attribute type
		nodeOccupancy = INTARRAYNONLEAFSIZE;
		leafOccupancy = INTARRAYLEAFSIZE;
	}
	else if (attributeType == 1){
		//1 = double attribute type
		nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		leafOccupancy = DOUBLEARRAYLEAFSIZE;
	}
	else if (attributeType == 2){
		//2 = string attribute type
		nodeOccupancy = STRINGARRAYNONLEAFSIZE;
		leafOccupancy = STRINGARRAYLEAFSIZE;
	}
	else {
		//should never occur
		std::cout << "none of the attribute types matched";
		//throw exception
	}
	if (File::exists(indexName)){

		file = new BlobFile(indexName, false);
		IndexMetaInfo* imi;
		Page* tempPage;
		PageId tempPageId;
		bufMgr->allocPage(file, tempPageId, tempPage);
		bufMgr->readPage(file, tempPageId, tempPage);
		imi = (IndexMetaInfo*)tempPage;

	} else {
		file = new BlobFile(indexName, true);
		Page* tempPage;
		Page* tempMetaPage;
		IndexMetaInfo* imi;
		bufMgr->readPage(file, headerPageNum, tempMetaPage);
		imi = (IndexMetaInfo *)tempMetaPage;
		rootPageNum = (*imi).rootPageNo;
		if ((*imi).attrByteOffset != this->attrByteOffset
				|| (*imi).attrType != this->attributeType
				|| (*imi).relationName != relationName
				|| (*imi).rootPageNo != rootPageNum
		){
			throw BadIndexInfoException("BadIndexInfoException\n the shit did not work");
		}

	}
	/*try {
		file = new BlobFile(indexName, false);
		//try to find the file?


	}catch (FileNotFoundException fnfe){
		//create the new file, which is declared in the header btree.h
		std::cout << "file not found exception \n";
		file = new BlobFile(indexName, true);
	}*/

	outIndexName = indexName;
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	//flip the scanExecuting bool
	scanExecuting = false;
	bufMgr->flushFile(file);
	delete file; // called to trigger the blobfile class's destructor
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

	if (rootPageNum == Page::INVALID_NUMBER){
		if( attributeType == INTEGER)
			{
				RIDKeyPair<int> entry;
				entry.set(rid, *((int *) key));
				insertIntegerRoot(entry);
			}
			else if (attributeType == DOUBLE)
			{
				RIDKeyPair<double> entry;
				entry.set(rid, *((double *) key));
				insertDoubleRoot(entry);
			}
			else if (attributeType == STRING)
			{
				RIDKeyPair<char> entry;
				entry.set(rid, *((char * )key));
				insertStringRoot(entry);
			}
	}
	else {
		//travers and then insert
	}
}

// ----------------------------------------------------------------------------
//BTreeIndex::insertInteger
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertIntegerRoot(RIDKeyPair<int> entry)
{

}


// ----------------------------------------------------------------------------
//BTreeIndex::insertDouble
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertDoubleRoot(RIDKeyPair<double> entry)
{


}

// ----------------------------------------------------------------------------
//BTreeIndex::insertString
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertStringRoot(RIDKeyPair<char> entry)
{


}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
