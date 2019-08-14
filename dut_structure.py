
from cdb import ue, mail, misc, sqlapi, auth, CADDOK, cdbtime, util, cmsg
from cdb.objects import Object, cdb_file
from cdb.ddl import Integer
from cdb import CADDOK
from mh.pvs.elaphe import barcode
from mh.pvs.pystrich.datamatrix import DataMatrixEncoder
from cdb.sqlapi import RecordSet2
import datetime
import os
import re
from PIL import Image, ImageFont, ImageDraw
from io import BytesIO
from cdb.plattools import killableprocess
import subprocess
from cs.vp.items import  Item
from cs.pcs.projects import Project
from asn1crypto._ffi import null
import time
import thread



class Mannhummelrelationdut(Object):
    __classname__ = "mh_pvs_rel_dut2dut"
    __maps_to__ = 'mh_pvs_rel_dut2dut'
    dut_top_value = ""
    # struct_id = ""
    
    dut_rel_field_values = {}

    top_assy_list = []
    def getAllTopAsseyDuts(self, dut_id1, dut_id2):
        """Finds the top assembly DuTs of dut_id1.
        
            Args:
                dut_id1: The target DuT.
                dut_id2: The source DuT.
            Returns:
                Returns a list of top DuTs
            
        """
        # getAllAssysSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID2= '" + dut_id1 + "' AND DUT_TYPE1='Assembly'"
        # allAssysRows = sqlapi.RecordSet2(sql=getAllAssysSQL)
        allAssysRows = Mannhummelrelationdut.KeywordQuery(dut_id2=dut_id1, dut_type1='Assembly')
        if(len(allAssysRows) > 0):
            for rel in allAssysRows:
                Mannhummelrelationdut.top_assy_list.append(rel.dut_id1)
                self.getAllTopAsseyDuts(rel.dut_id1, dut_id2)
        return Mannhummelrelationdut.top_assy_list
    
    
      
    parts_list = []
    assy_list = set()
    def checkIfPartAlreadyExistInAssy(self, dut_id1, dut_id2):
        """Checks if the top assembly dut_id1 has parts, if found create a list of parts.
        
        Args:
            dut_id1: The target DuT.
            dut_id2: The dragged DuT.
            
        Returns:
            A list of Part DuTs.
            
        """
        allAssysRows = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id1)
        # getAllAssysSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID1= '" + dut_id1 + "' AND DUT_TYPE2='Assembly'"
        # allAssysRows = sqlapi.RecordSet2(sql=getAllAssysSQL)
        '''When there are no sub-assembly then get all the parts and 
        if the parts contians the dragged element then raise exception
        else call the fucnation recursively. 
        '''
        if(len(allAssysRows) == 0):
            # getAllAssyPartsSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID1= '" + dut_id1 + "' AND DUT_TYPE2='Part'"
            # allAssyPartsRows = sqlapi.RecordSet2(sql=getAllAssyPartsSQL)
            allAssyPartsRows = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id1, dut_type2='Part')
            if(len(allAssyPartsRows) > 0):
                for rel in allAssyPartsRows:
                   # getting all inner parts
                   Mannhummelrelationdut.parts_list.append(rel.dut_id2)
        elif(len(allAssysRows) > 0):
            # call recursively
            for rel in allAssysRows:
                '''checking if the same assembly is dropped again, if so raise exception else call again
                '''
                if(rel.dut_type2 == "Assembly"):
                    Mannhummelrelationdut.assy_list.add(rel.dut_id2)
                    if(rel.dut_id2 == dut_id2 or rel.dut_id2 == dut_id1):
                       message = "Assembly " + dut_id2 + " is already in a relation with " + rel.dut_id1
                       raise ue.Exception(1024, message) 
                    self.checkIfPartAlreadyExistInAssy(rel.dut_id2, dut_id2)
                elif(rel.dut_type2 == "Part"):
                    Mannhummelrelationdut.parts_list.append(rel.dut_id2)
        return Mannhummelrelationdut.parts_list
    
    
    def checkIfAssyAlreadyExistInAssy(self, dut_id1, dut_id2):
        """When there are no sub-assembly then get all the parts of target & source DuT and 
        if the parts already exist then raise exception else call the fucnation recursively.
        
        Args:
            dut_id1: The target DuT.
            dut_id2: The dragged DuT.
        Returns:
            No return value
        """
        allAssysRows = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id2)
        # getAllAssysSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID1= '" + dut_id2 + "' AND DUT_TYPE2='Assembly'"
        # allAssysRows = sqlapi.RecordSet2(sql=getAllAssysSQL)
        if(len(allAssysRows) == 0):
            # getAllAssyPartsSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID1= '" + dut_id2 + "' AND DUT_TYPE2='Part'"
            # allAssyPartsRows = sqlapi.RecordSet2(sql=getAllAssyPartsSQL)
            allAssyPartsRows = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id2, dut_type2='Part')
            if(len(allAssyPartsRows) > 0):
                for rel in allAssyPartsRows:
                   # getting all inner parts
                   Mannhummelrelationdut.parts_list.append(rel.dut_id2)
                    
        elif(len(allAssysRows) > 0):
            # call recursively
            for rel in allAssysRows:
                # self.checkIfPartAlreadyExistInAssy(rel.dut_id2, dut_id2)
                if(rel.dut_type2 == "Assembly"):
                    if(rel.dut_id2 in Mannhummelrelationdut.assy_list):
                        message = rel.dut_id2 + " is already in a relation with " + dut_id1
                        raise ue.Exception(1024, message) 
                    self.checkIfAssyAlreadyExistInAssy(dut_id2, rel.dut_id2)
                elif(rel.dut_type2 == "Part"):    
                    Mannhummelrelationdut.parts_list.append(rel.dut_id2)
        return Mannhummelrelationdut.parts_list;
    
    
    def deleteTheDraggedRelationLink(self, dut_id1, dut_id2):
        """Deletes the relation between dut_id1 and dut_id2.

        Args:
            dut_id1: The target DuT.
            dut_id2: The dragged DuT.
        Returns:
            No return value
    
        """
        # relToBeDeletedSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID1= '" + dut_id2 + "'"
        relToBeDeletedRows = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id1, dut_id2=dut_id2)
        if(len(relToBeDeletedRows) > 0):
            for rel in relToBeDeletedRows:
                rel.Delete()
        pass
    
    
    is_same_struct_global = False
    k = 0
    dut_id1_del = dut_id2_del = ""
    def isOperationInSameStruct(self, dut_id1, dut_id2, existing_assys_list):
        """Check if the drag drop operation is in the same structure.
        
        Case: If any of the source DuT's top assemblies matches with the target DuT's 
        top assembly, then it is considered that the drag-drop operation happens in same structure,
        else different structure.

        Args:
            dut_id1: The target DuT.
            dut_id2: The dragged DuT.
            existing_assys_list: list of top parent DuT Assemblies of the source DuT.
        Returns:
            boolean. True if operation in same structure, False otherwise.
    
        """
        # getAllAssysSQL = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID2= '" + dut_id2 + "' AND DUT_TYPE1='Assembly'"
        # getAllAssysRows = sqlapi.RecordSet2(sql=getAllAssysSQL)
        getAllAssysRows = Mannhummelrelationdut.KeywordQuery(dut_id2=dut_id2, dut_type1='Assembly')
        if(len(getAllAssysRows) > 0):
            for rel in getAllAssysRows:
                if (Mannhummelrelationdut.k == 0):
                    Mannhummelrelationdut.dut_id1_del = rel.dut_id1
                    Mannhummelrelationdut.dut_id2_del = dut_id2
                if(rel.dut_id1 in existing_assys_list):
                    Mannhummelrelationdut.is_same_struct_global = True
                    self.deleteTheDraggedRelationLink(Mannhummelrelationdut.dut_id1_del, Mannhummelrelationdut.dut_id2_del)
                else:
                    Mannhummelrelationdut.k += 1
                    self.isOperationInSameStruct(dut_id1, rel.dut_id1, existing_assys_list)
        return Mannhummelrelationdut.is_same_struct_global            
    
    

    def checkIfDutDroppedToExistingChildAssy(self, dut_id2):
        """check if the source DuT is dropped to its immidiate inner childs,
            if exist raise exception
           
           Args:
               dut_id2: The source DuT.
            Returns:
                No return value
        """
        existingRelRows3 = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id2)
        if (len(existingRelRows3) > 0):
            for rel in existingRelRows3:
                if (rel.dut_id2 == Mannhummelrelationdut.dut_id1_del):
                    message = Mannhummelrelationdut.dut_id2_del + " is already in relation with " + Mannhummelrelationdut.dut_id1_del
                    raise ue.Exception(1024, message)
                Mannhummelrelationdut.k += 1
                self.checkIfDutDroppedToExistingChildAssy(rel.dut_id2)


    def checkIfDutDraggedToExistingAssy(self, dut_id1, dut_id2):
        """Checks and raises exception if the source & target DuTs are in the
            same structure for the following cases
            case 1: If the source DuT is dropped to its immidiate parent, 
                    if not handled it goes recursively recursive. 
            case 2: If the source DuT is dropped to its immidiate child,
                    if not handled it goes recursively recursive.
            case 3: If the source DuT is dropped to its immidiate inner childs,
                    if not handled it goes recursively recursive.
            
            Args:
                dut_id1: The target DuT.
                dut_id2: The dragged DuT.
            Returns:
                No return value.
        """
        if (Mannhummelrelationdut.k == 0):
            Mannhummelrelationdut.dut_id1_del = dut_id1
            Mannhummelrelationdut.dut_id2_del = dut_id2
        existingRelRows1 = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id1, dut_id2=dut_id2)
        if(len(existingRelRows1) > 0):
            if(existingRelRows1[0].dut_id1 == dut_id1):
                message = Mannhummelrelationdut.dut_id2_del + " is already in relation with " + Mannhummelrelationdut.dut_id1_del
                raise ue.Exception(1024, message)
        else:
            existingRelRows2 = Mannhummelrelationdut.KeywordQuery(dut_id1=dut_id2, dut_id2=dut_id1)
            if(len(existingRelRows2) > 0):
                if(existingRelRows2[0].dut_id1 == Mannhummelrelationdut.dut_id2_del):
                    message = Mannhummelrelationdut.dut_id2_del + " is already in relation with " + Mannhummelrelationdut.dut_id1_del
                    raise ue.Exception(1024, message)
            else:
                self.checkIfDutDroppedToExistingChildAssy(dut_id2)
    




def drag_drop_relation_creation(self, ctx): 
    """ Checks the following points and validates if a relation between two DuTs is valid.
    RULES:
    1. Assembly to Assembly : Possible
    2. Assembly to Part : Possible, not the reverse
    3. Part to Sub-Part : Possible, not the reverse
    4. Assembly to Subpart : Not Possible
    5. A Sub-Part of a Part can't be added to other Part
    6. A Part can be part of many Assemblys
    7. When making Assy to Assy & Assy to Part relation within same structure the old Assy rel should be deleted & new Assy rel shoud be created
    8. When making Assy to Assy & Assy to Part relation not within same structure the old Assy rel should be copied & new Assy rel shoud be created
    9. A Structure should only contain unique Parts, Sub-Parts, Assemblys
    10. Sub-Part to Sub-Part : Not possible
    """
    rel_list = []
    dut_id1 = ctx.dialog['dut_id1']
    dut_id2 = ctx.dialog['dut_id2']
    dut_type1 = ctx.dialog['dut_type1']
    dut_type2 = ctx.dialog['dut_type2']
    
    '''1st check'''
    # If a source and target DuT are same.
    if (dut_id1 == dut_id2):
        message = "A relation connot established between same objects"
        raise ue.Exception(1024, message)
    
    '''2nd check'''
    Mannhummelrelationdut.k = 0
    self.checkIfDutDraggedToExistingAssy(dut_id1, dut_id2)
    
    result = sqlapi.RecordSet2("mh_pvs_dut", "dut_id='" + dut_id1 + "'")
    idvalue = result[0]
    dut_type1 = getattr(idvalue, "dut_type")
    ctx.set('dut_type1', dut_type1) 
    
    '''3rd check'''
    self.checkattributevalues(ctx)
    
    '''4th check'''
    if(dut_type2 == "Sub-Part"):
        # sql = "SELECT * FROM MH_PVS_REL_DUT2DUT WHERE DUT_ID2 = '" + dut_id2 + "' and DUT_TYPE2='Sub-Part'"
        # result = sqlapi.RecordSet2(sql=sql)
        result = Mannhummelrelationdut.KeywordQuery(dut_id2=dut_id2, dut_type2='Sub-Part')
        if(len(result) > 0):
            message = dut_id2 + " is already in a relation with " + result[0].dut_id1
            raise ue.Exception(1024, message)
      
    if(dut_type2 == "Part"):
        '''adding chek for drag-drop within the same structure'''
        Mannhummelrelationdut.top_assy_list.append(dut_id1)
        Mannhummelrelationdut.k = 0
        existing_assys_list = self.getAllTopAsseyDuts(dut_id1, dut_id2)
        Mannhummelrelationdut.top_assy_list = []
        is_same_struct = self.isOperationInSameStruct(dut_id1, dut_id2, existing_assys_list)
        Mannhummelrelationdut.is_same_struct_global = False
        if(not is_same_struct):
            existing_parts_list = self.checkIfPartAlreadyExistInAssy(dut_id1, dut_id2)
            if(dut_id2 in existing_parts_list):
                message = "Part " + dut_id2 + " already exist in Assembly " + dut_id1
                raise ue.Exception(1024, message)
        
    if(dut_type2 == "Assembly"):
        Mannhummelrelationdut.assy_list.clear()
        Mannhummelrelationdut.k = 0
        Mannhummelrelationdut.top_assy_list.append(dut_id1)
        existing_assys_list = self.getAllTopAsseyDuts(dut_id1, dut_id2)
        Mannhummelrelationdut.top_assy_list = []
        is_same_struct = self.isOperationInSameStruct(dut_id1, dut_id2, existing_assys_list)
        Mannhummelrelationdut.is_same_struct_global = False
        if(not is_same_struct):
            # get the parts list of the target DuT
            existing_parts_list = self.checkIfPartAlreadyExistInAssy(dut_id1, dut_id2)
            Mannhummelrelationdut.parts_list = []
            # get the parts list of the source DuT
            dragged_parts_list = self.checkIfAssyAlreadyExistInAssy(dut_id1, dut_id2)
            Mannhummelrelationdut.parts_list = []
            duts_already_exist = set(existing_parts_list) & set(dragged_parts_list)
            if(len(duts_already_exist) > 0):
                messages = []
                for ele in duts_already_exist:
                    message = "Part " + ele + " already exist in Assembly " + dut_id1 
                    messages.append(message)
                if(len(messages) > 0):
                    raise ue.Exception(1024, '\n'.join(messages))