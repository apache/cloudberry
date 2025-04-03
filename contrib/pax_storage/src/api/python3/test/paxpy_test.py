# python3
import unittest
import paxpy
import datetime

"""
    test.file1: TEXTOID(25), TEXTOID(25), INT4OID(23)
        - 1 group
        - 1 row
        - no null rows
    test.file2: INT2OID(21),INT4OID(23),INT8OID(20),FLOAT4OID(700),FLOAT8OID(701),BYTEAOID(17),TEXTOID(25),
        VARCHAROID(1043),BOOLOID(16),DATEOID(1082),TIMEOID(1083),TIMESTAMPOID(1114),TIMESTAMPTZOID(1184)
        - 3 groups (every group per 5 rows)
        - 11 row
        - no null rows
    test.file3: INT4OID(23)
        - 9 groups (every group per 5 rows)
        - 46 rows
        - Even rows are null, 23 null rows
    test.file4: INT4OID(23)
        - 1 group
        - 100 rows
        - all rows are null
        - vm1: delete from testnull where v1 in (1, 3, 5, 9, 11);
        - vm2: delete from testnull where v1 in (1, 3, 5, 9, 11) + ctid <= 8
    test.file5: INT2ARRAYOID(1005), INT4ARRAYOID(1007), INT8ARRAYOID(1016), FLOAT4ARRAYOID(1021), FLOAT4ARRAYOID(1022)
        - 2 groups
        - 6 rows
        - no null rows
              v1      |        v2        |    v3     |           v4           |           v5
        --------------+------------------+-----------+------------------------+------------------------
         {1,2,3,4}    | {1,2,3,4}        | {1,2,3,4} | {1.1,2.2,3.3,4.4,5.5}  | {1.1,2.2,3.3,4.4,5.5}
         {2,3,4,5}    | {2,3,4,5}        | {2,3,4,5} | {2.2,3.3,4.4,5.5,6.6}  | {2.2,3.3,4.4,5.5,6.6}
         {3,4,5,6}    | {3,4,5,6}        | {3,4,5,6} | {3.3,4.4,5.5,6.6,7.7}  | {3.3,4.4,5.5,6.6,7.7}
         {4,5,6,7}    | {4,5,6,7}        | {4,5,6,7} | {4.4,5.5,6.6,7.7,8.8}  | {4.4,5.5,6.6,7.7,8.8}
         {5,6,7,8}    | {5,6,7,8}        | {5,6,7,8} | {5.5,6.6,7.7,8.8,9.9}  | {5.5,6.6,7.7,8.8,9.9}
         {6,7,8,9}    | {6,7,8,9}        | {6,7,8,9} | {6.6,7.7,8.8,9.9,10.1} | {6.6,7.7,8.8,9.9,10.1}
    test.file6: INT2ARRAYOID(1005), INT4ARRAYOID(1007), INT8ARRAYOID(1016), FLOAT4ARRAYOID(1021), FLOAT4ARRAYOID(1022)
        - 1 group
        - 1 row
        - no null rows, but some of null in the array type
              v1      |        v2        |    v3     |           v4           |           v5
        --------------+------------------+-----------+------------------------+------------------------
         {1,2,NULL,4} | {NULL,NULL,NULL} | {1,2,3,4} | {1.1,2.2,3.3,4.4,5.5}  | {1.1,2.2,3.3,NULL,5.5}
    test.file7: TEXTOID(25), TEXTOID(25), TEXTOID(25)
        - 1 group
        - 1 row
        - no null rows
        - with toast file
              v1       |      v2       |       v3      |
        ---------------+---------------+---------------+
         len(51111111) | len(52222222) | len(53333333) |
    test.file8: INT2OID(21),INT4OID(23),INT8OID(20),FLOAT4OID(700),FLOAT8OID(701),BYTEAOID(17),TEXTOID(25),
        VARCHAROID(1043),BOOLOID(16),DATEOID(1082),TIMEOID(1083),TIMESTAMPOID(1114),TIMESTAMPTZOID(1184),
        INT2ARRAYOID(1005), INT4ARRAYOID(1007), INT8ARRAYOID(1016), FLOAT4ARRAYOID(1021), FLOAT4ARRAYOID(1022)
        - 2 group
        - 6 row
        - no null rows
        - porc_vec format
    test.file9: [23,25,25,1007,3802]
        - 1 group 1 row
        - last column is jsonb which not support
"""
class TestPaxpy(unittest.TestCase):
    def test_basic(self):
        # TEXTOID(25), TEXTOID(25), INT4OID(23)
        schema = [25,25,23]
        proj = [True, True, True]
        pax_file = paxpy.paxfile('test.file1')
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertEqual(len(g0[0]), 1)
        pax_file_reader.close()
    
    def test_read_group_twice(self):
        # TEXTOID(25), TEXTOID(25), INT4OID(23)
        schema = [25,25,23]
        proj = [True, True, True]
        pax_file = paxpy.paxfile('test.file1')
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertEqual(len(g0[0]), 1)
        
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertEqual(len(g0[0]), 1)
        pax_file_reader.close()
    
    def test_projection(self):
        # TEXTOID(25), TEXTOID(25), INT4OID(23)
        schema = [25,25,23]
        
        pax_file = paxpy.paxfile('test.file1')

        proj = [False, True, True]
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertIsNone(g0[0])
        self.assertIsNotNone(g0[1])
        self.assertIsNotNone(g0[2])
        pax_file_reader.close()

        proj = [True, False, True]
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertIsNotNone(g0[0])
        self.assertIsNone(g0[1])
        self.assertIsNotNone(g0[2])
        pax_file_reader.close()

        proj = [True, True, False]
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertIsNotNone(g0[0])
        self.assertIsNotNone(g0[1])
        self.assertIsNone(g0[2])
        pax_file_reader.close()

        proj = [False, False, False]
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertIsNone(g0[0])
        self.assertIsNone(g0[1])
        self.assertIsNone(g0[2])
        pax_file_reader.close()

    def test_type_support(self):
        # INT2OID(21),INT4OID(23),INT8OID(20),FLOAT4OID(700),FLOAT8OID(701),BYTEAOID(17),TEXTOID(25),
        # VARCHAROID(1043),BOOLOID(16),DATEOID(1082),TIMEOID(1083),TIMESTAMPOID(1114),TIMESTAMPTZOID(1184)

        schema = [21,23,20,700,701,17,25,1043,16,1082,1083,1114,1184]
        pax_file = paxpy.paxfile('test.file2')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        # 3 groups
        self.assertEqual(pax_file_reader.getgroupnums(), 3)
        g0 = pax_file_reader.readgroup(0)
        g1 = pax_file_reader.readgroup(1)
        g2 = pax_file_reader.readgroup(2)

        self.assertEqual(len(schema), len(g0))
        self.assertEqual(len(g0[0]), 5)
        self.assertEqual(len(schema), len(g1))
        self.assertEqual(len(g1[0]), 5)
        self.assertEqual(len(schema), len(g2))
        self.assertEqual(len(g2[0]), 1)

        self.assertEqual(g2[0][0], 11)
        self.assertEqual(g2[1][0], 11)
        self.assertEqual(g2[2][0], 11)
        # self.assertEqual(g2[3][0], 11.11) -- Float4 point precision is different
        self.assertEqual(g2[4][0], 11.11)
        self.assertEqual(g2[5][0], b"11")
        self.assertEqual(g2[6][0], "11")
        self.assertEqual(g2[7][0], "11")
        self.assertEqual(g2[8][0], True)
        self.assertEqual(g2[9][0], datetime.date(2000, 1, 11))
        self.assertEqual(g2[10][0], datetime.time(0, 0, 11))
        self.assertEqual(g2[11][0], datetime.datetime(2000, 1, 11, 0, 0, 11))
        self.assertEqual(g2[12][0], datetime.datetime(2000, 1, 11, 0, 0, 11))

        pax_file_reader.close()

    def test_unsupport_type(self):
        schema = [23, 25, 25, 1007, 3802]
        proj = [False, False, False, False, True]

        pax_file = paxpy.paxfile('test.file9')
        pax_file_reader = paxpy.paxfilereader(schema, proj, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)

        self.assertEqual(len(schema), len(g0))
        self.assertIsNone(g0[0])
        self.assertIsNone(g0[1])
        self.assertIsNone(g0[2])
        self.assertIsNone(g0[3])
        self.assertIsNotNone(g0[4])

    def test_null_rows(self):
        # INT4OID(23)
        schema = [23]
        pax_file = paxpy.paxfile('test.file3')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        # 3 groups
        self.assertEqual(pax_file_reader.getgroupnums(), 9)
        g0 = pax_file_reader.readgroup(0)
        g1 = pax_file_reader.readgroup(1)
        g2 = pax_file_reader.readgroup(2)

        self.assertEqual(len(schema), len(g0))
        self.assertEqual(len(g0[0]), 5)
        self.assertEqual(len(schema), len(g1))
        self.assertEqual(len(g1[0]), 5)
        self.assertEqual(len(schema), len(g2))
        self.assertEqual(len(g2[0]), 5)

        self.assertEqual(g0[0][0], 1)
        self.assertEqual(g0[0][1], None)
        self.assertEqual(g0[0][2], 3)
        self.assertEqual(g0[0][3], None)
        self.assertEqual(g0[0][4], 5)

        self.assertEqual(g1[0][0], None)
        self.assertEqual(g1[0][1], 7)
        self.assertEqual(g1[0][2], None)
        self.assertEqual(g1[0][3], 9)
        self.assertEqual(g1[0][4], None)
    
    def test_all_null(self):
        # INT4OID(23)
        schema = [23]
        pax_file = paxpy.paxfile('test.file4')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(g0), 1)
        self.assertEqual(len(g0[0]), 100)
        for r in g0[0]:
            self.assertEqual(r, None)


    def test_array_type(self):
        # INT2ARRAYOID(1005), INT4ARRAYOID(1007), INT8ARRAYOID(1016), FLOAT4ARRAYOID(1021), FLOAT4ARRAYOID(1022)
        schema = [1005, 1007, 1016, 1021, 1022]
        pax_file = paxpy.paxfile('test.file5')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 2)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(g0), 5) # 5 columns
        self.assertEqual(len(g0[0]), 5) # 5 rows

        self.assertEqual(len(g0[0][0]), 4) # 4 elements in array
        self.assertEqual(len(g0[1][1]), 4)
        self.assertEqual(len(g0[2][2]), 4)
        self.assertEqual(len(g0[3][3]), 5)
        self.assertEqual(len(g0[4][4]), 5)

        self.assertListEqual(g0[0][0], [1,2,3,4])
        self.assertListEqual(g0[1][1], [2,3,4,5])
        self.assertListEqual(g0[2][2], [3,4,5,6])
        self.assertListEqual(g0[4][4], [5.5,6.6,7.7,8.8,9.9])

    def test_array_exist_null(self):
        schema = [1005, 1007, 1016, 1021, 1022]
        pax_file = paxpy.paxfile('test.file6')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(g0), 5) 
        self.assertEqual(len(g0[0]), 1)

        self.assertListEqual(g0[0][0], [1,2,None,4])
        self.assertListEqual(g0[1][0], [None,None,None])
        self.assertListEqual(g0[2][0], [1,2,3,4])
        self.assertListEqual(g0[4][0], [1.1,2.2,3.3,None,5.5])

    def test_visible_map(self):
        # INT4OID(23)
        schema = [23]
        pax_file = paxpy.paxfile('test.file3', 'test.file3.vm1')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 9)
        g0 = pax_file_reader.readgroup(0)
        g1 = pax_file_reader.readgroup(1)
        g2 = pax_file_reader.readgroup(2)
        g3 = pax_file_reader.readgroup(3)
        g4 = pax_file_reader.readgroup(4)
        g5 = pax_file_reader.readgroup(5)
        g6 = pax_file_reader.readgroup(6)
        g7 = pax_file_reader.readgroup(7)
        g8 = pax_file_reader.readgroup(8)

        self.assertEqual(len(g0), 1)
        self.assertEqual(len(g1), 1)
        self.assertEqual(len(g2), 1)
        self.assertEqual(len(g3), 1)
        self.assertEqual(len(g4), 1)
        self.assertEqual(len(g5), 1)
        self.assertEqual(len(g6), 1)
        self.assertEqual(len(g7), 1)
        self.assertEqual(len(g8), 1)

        self.assertEqual(g0[0], [None, None])
        self.assertEqual(g1[0], [None, 7, None, None])
        self.assertEqual(g2[0], [None, 13, None, 15])
        self.assertEqual(g3[0], [None, 17, None, 19, None])
        self.assertEqual(g4[0], [21, None, 23, None, 25])
        self.assertEqual(g5[0], [None, 27, None, 29, None])
        self.assertEqual(g6[0], [31, None, 33, None, 35])
        self.assertEqual(g7[0], [None, 37, None, 39, None])
        self.assertEqual(g8[0], [41])

    def test_empty_group(self):
        # INT4OID(23)
        schema = [23]
        pax_file = paxpy.paxfile('test.file3', 'test.file3.vm2')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 9)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(g0), 1)
        self.assertEqual(len(g0[0]), 0) # all deleted

    def test_toast(self):
        # TEXTOID(25), TEXTOID(25), TEXTOID(25)
        schema = [25,25,25]
        pax_file = paxpy.paxfile('test.file7',  toastfilepath='test.file7.toast')

        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(g0), len(schema))
        self.assertEqual(len(g0[0]), 1)
        self.assertEqual(len(g0[1]), 1)
        self.assertEqual(len(g0[2]), 1)

        self.assertEqual(len(g0[0][0]),51111111)
        toast_str = g0[0][0]
        for toast_str_slice in toast_str[0:1000]:
            self.assertEqual(toast_str_slice, '0')
        self.assertEqual(len(g0[1][0]),52222222)
        self.assertEqual(len(g0[2][0]),53333333)

    def test_drop_columns(self):
        # TEXTOID(25), TEXTOID(25), INT4OID(23)
        # Pax file have three columns, but column index 1 have been dropped
        schema = [25,0,23]
        pax_file = paxpy.paxfile('test.file1')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertEqual(g0[1], None)
        pax_file_reader.close()

    def test_add_columns(self):
        # TEXTOID(25), TEXTOID(25), INT4OID(23)
        # Pax file have three columns, but current table do the `add column`
        schema = [25,25,23,23]
        pax_file = paxpy.paxfile('test.file1')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 1)
        g0 = pax_file_reader.readgroup(0)
        self.assertEqual(len(schema), len(g0))
        self.assertEqual(g0[3], None)
        pax_file_reader.close()

    def test_vec_format(self):
        schema = [21,23,20,700,701,17,25,1043,16,1082,1083,1114,1184,1005,1007, 1016, 1021, 1022]
        pax_file = paxpy.paxfile('test.file8')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        self.assertEqual(pax_file_reader.getgroupnums(), 2)
        g0 = pax_file_reader.readgroup(0)
        g1 = pax_file_reader.readgroup(1)
        self.assertEqual(len(schema), len(g0))
        self.assertEqual(len(schema), len(g1))

        self.assertListEqual(g0[0], [1,2,3,4,5])
        self.assertListEqual(g0[1], [1,2,3,4,5])
        self.assertListEqual(g0[2], [1,2,3,4,5])
        # self.assertListEqual(g0[3], [1.1,2.2,3.3,4.4,5.5]) # float4
        self.assertListEqual(g0[4], [1.1,2.2,3.3,4.4,5.5])
        self.assertListEqual(g0[5], [b'1',b'2',b'3',b'4',b'5'])
        self.assertListEqual(g0[6], ['1','2','3','4','5'])
        self.assertListEqual(g0[7], ['1','2','3','4','5'])
        self.assertListEqual(g0[8], [True,False,True,False,True])
        self.assertListEqual(g0[9], [datetime.date(2000, 1, 1),datetime.date(2000, 1, 2),datetime.date(2000, 1, 3),datetime.date(2000, 1, 4),datetime.date(2000, 1, 5)])
        self.assertListEqual(g0[10], [datetime.time(0, 0, 1),datetime.time(0, 0, 2),datetime.time(0, 0, 3),datetime.time(0, 0, 4),datetime.time(0, 0, 5)])
        self.assertListEqual(g0[11], [datetime.datetime(2000, 1, 1, 0, 0, 1),datetime.datetime(2000, 1, 2, 0, 0, 2),datetime.datetime(2000, 1, 3, 0, 0, 3),datetime.datetime(2000, 1, 4, 0, 0, 4),datetime.datetime(2000, 1, 5, 0, 0, 5)])
        self.assertListEqual(g0[12], [datetime.datetime(2000, 1, 1, 0, 0, 1),datetime.datetime(2000, 1, 2, 0, 0, 2),datetime.datetime(2000, 1, 3, 0, 0, 3),datetime.datetime(2000, 1, 4, 0, 0, 4),datetime.datetime(2000, 1, 5, 0, 0, 5)])
        self.assertListEqual(g0[13], [[1,2,3,4], [2,3,4,5], [3,4,5,6],[4,5,6,7],[5,6,7,8]])
        self.assertListEqual(g0[14], [[1,2,3,4], [2,3,4,5], [3,4,5,6],[4,5,6,7],[5,6,7,8]])
        self.assertListEqual(g0[15], [[1,2,3,4], [2,3,4,5], [3,4,5,6],[4,5,6,7],[5,6,7,8]])
        # self.assertListEqual(g0[16], [[1.1,2.2,3.3,4.4,5.5], [2.2,3.3,4.4,5.5,6.6], [3.3,4.4,5.5,6.6,7.7],[4.4,5.5,6.6,7.7,8.8],[5.5,6.6,7.7,8.8,9.9]]) # float4 array
        self.assertListEqual(g0[17], [[1.1,2.2,3.3,4.4,5.5], [2.2,3.3,4.4,5.5,6.6], [3.3,4.4,5.5,6.6,7.7],[4.4,5.5,6.6,7.7,8.8],[5.5,6.6,7.7,8.8,9.9]])

        pax_file_reader.close()

    def test_exceptions(self):
        ex_catched = False
        # case 1: wrong schema
        schema = [21,23,20,700,701,17,25,1043,16,1082,1083,1114,1184,1005, 1007, 1016, 1021, 1022]
        pax_file = paxpy.paxfile('test.file1')
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        try:
            g0 = pax_file_reader.readgroup(0)
        except:
            ex_catched = True
        self.assertEqual(ex_catched, True)
        
        # case 2: group index out of range
        schema = [25,25,23]
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        ex_catched = False
        try:
            g = pax_file_reader.readgroup(1000)
        except:
            ex_catched = True
        self.assertEqual(ex_catched, True)

        # case 3: catch pg error
        schema = [1005,1005,1005]
        pax_file_reader = paxpy.paxfilereader(schema, None, pax_file)
        ex_catched = False
        try:
            # cbdb::ArrayGetN will got error
            g = pax_file_reader.readgroup(0)
        except:
            ex_catched = True
        self.assertEqual(ex_catched, True)

if __name__ == '__main__':
    unittest.main(verbosity=2)
