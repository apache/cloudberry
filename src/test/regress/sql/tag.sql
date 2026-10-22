-- Test tag manipulation
\d+ pg_tag;
\d+ pg_tag_description;
SELECT * FROM pg_tag;
SELECT * FROM pg_tag_description;
CREATE DATABASE other_db;

-- Test create tag
CREATE TAG tag1;
CREATE TAG IF NOT EXISTS tag1;
CREATE TAG IF NOT EXISTS tag2;
CREATE TAG tag2;    -- error
CREATE TAG tag3 ALLOWED_VALUES '123';
CREATE TAG tag3;    -- error
CREATE TAG tag4 ALLOWED_VALUES '123', '456', ' ';
CREATE TAG IF NOT EXISTS tag5 ALLOWED_VALUES '123', 'val1';
CREATE TAG tag6 ALLOWED_VALUES 'nqwenfqpjenpjqnpufnqwpiuenfuiasqwefqfsafqwefnfiunl;jfa;lskdfjqpwefjqpewe234dfqwef'
'fjhsudfiueqihfsakdljfqeqfhqoufhoaisuehfqoiuwehfqoifhqoiuwehfoqihfosfqwfeqwfqwefqwefqwfefqefqfqewfqwefweqfsdfqwef'
'qifquhowifhoiuqhfuosdfqjfqkepfqjfpishdfuiqh2139u108wefoiuqwhefoiuqwehfoiuqwhfoiuweqfheiasuohfioquwehfqwoeiufhdas'
'iqeqwejfpqwifjipqwfjqwiefjpiqwehfpiquwehfqwiufhqwpiuefhqwui0fhpiiohfoqiuwehfoquwefoiweqfewoqifqwoiufhho';          -- error
CREATE TAG tag7 ALLOWED_VALUES 'val1', '123', 'val1';     -- error
CREATE TAG tag8 ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', 
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300';
CREATE TAG tag9 ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',   -- error
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300', '301';

SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

-- Test Drop tag
DROP TAG tag5;
DROP TAG IF EXISTS tag5;
DROP TAG IF EXISTS tag8;
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

-- Test Alter tag
-- Rename
ALTER TAG tag3 RENAME TO tag3_new;
ALTER TAG IF EXISTS tag4 RENAME TO tag4_new;
ALTER TAG tag3_new RENAME TO tag3;
ALTER TAG tag4_new RENAME TO tag4;

SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

-- OWNER TO
CREATE USER tag_user;
ALTER TAG tag1 OWNER TO tag_user;
SELECT tagname, rolname AS tagowner, allowed_values FROM pg_tag, pg_authid
WHERE pg_tag.tagowner = pg_authid.oid
ORDER BY 1;
ALTER TAG tag1 OWNER TO gpadmin;
SELECT tagname, rolname AS tagowner, allowed_values FROM pg_tag, pg_authid
WHERE pg_tag.tagowner = pg_authid.oid
ORDER BY 1;
\c other_db
SELECT tagname, rolname AS tagowner, allowed_values FROM pg_tag, pg_authid
WHERE pg_tag.tagowner = pg_authid.oid
ORDER BY 1;
\c postgres

-- Unset allowed_values
ALTER TAG tag1 UNSET ALLOWED_VALUES;
ALTER TAG tag2 UNSET ALLOWED_VALUES;
ALTER TAG tag3 UNSET ALLOWED_VALUES;

SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

-- ADD allowed_values
ALTER TAG tag1 ADD ALLOWED_VALUES 'val1';
ALTER TAG tag1 ADD ALLOWED_VALUES 'val1';   -- error
ALTER TAG IF EXISTS tag1 ADD ALLOWED_VALUES 'val1';  -- error
ALTER TAG tag1 ADD ALLOWED_VALUES 'val2', 'val2';   -- error
ALTER TAG IF EXISTS tag1 ADD ALLOWED_VALUES 'val2', 'val2';   -- error
ALTER TAG tag1 ADD ALLOWED_VALUES 'val2', 'val3';
ALTER TAG IF EXISTS tag1 ADD ALLOWED_VALUES 'val4', 'val5';
ALTER TAG tag1 ADD ALLOWED_VALUES '', '';   -- error
ALTER TAG IF EXISTS tag1 ADD ALLOWED_VALUES '', ''; --error
ALTER TAG tag1 ADD ALLOWED_VALUES ' ', ' '; -- error
ALTER TAG IF EXISTS tag1 ADD ALLOWED_VALUES ' ';

ALTER TAG tag1 ADD ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',    -- error
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300';
ALTER TAG IF EXISTS tag1 ADD ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',    -- error
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300';
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

ALTER TAG tag2 ADD ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300';
ALTER TAG IF EXISTS tag2 ADD ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',    -- error
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300';

ALTER TAG tag3 ADD ALLOWED_VALUES 'nqwenfqpjenpjqnpufnqwpiuenfuiasqwefqfsafqwefnfiunl;jfa;lskdfjqpwefjqpewe234dfqwef'
'fjhsudfiueqihfsakdljfqeqfhqoufhoaisuehfqoiuwehfqoifhqoiuwehfoqihfosfqwfeqwfqwefqwefqwfefqefqfqewfqwefweqfsdfqwef'
'qifquhowifhoiuqhfuosdfqjfqkepfqjfpishdfuiqh2139u108wefoiuqwhefoiuqwehfoiuqwhfoiuweqfheiasuohfioquwehfqwoeiufhdas'
'iqeqwejfpqwifjipqwfjqwiefjpiqwehfpiquwehfqwiufhqwpiuefhqwui0fhpiiohfoqiuwehfoquwefoiweqfewoqifqwoiufhho';  -- error
ALTER TAG IF EXISTS tag3 ADD ALLOWED_VALUES 'nqwenfqpjenpjqnpufnqwpiuenfuiasqwefqfsafqwefnfiunl;jfa;lskdfjqpwefjqpewe234dfqwef'
'fjhsudfiueqihfsakdljfqeqfhqoufhoaisuehfqoiuwehfqoifhqoiuwehfoqihfosfqwfeqwfqwefqwefqwfefqefqfqewfqwefweqfsdfqwef'
'qifquhowifhoiuqhfuosdfqjfqkepfqjfpishdfuiqh2139u108wefoiuqwhefoiuqwehfoiuqwhfoiuweqfheiasuohfioquwehfqwoeiufhdas'
'iqeqwejfpqwifjipqwfjqwiefjpiqwehfpiquwehfqwiufhqwpiuefhqwui0fhpiiohfoqiuwehfoquwefoiweqfewoqifqwoiufhho';  -- error
ALTER TAG tag3 ADD ALLOWED_VALUES 'dadkqjefpqfqfqe4l123j9i1snkqenp3412n4jnflqjenfaddpiqepj21304i12;kfnqpqnepfqefqwef';

ALTER TAG tag4 ADD ALLOWED_VALUES ' ';  -- error
ALTER TAG IF EXISTS tag4 ADD ALLOWED_VALUES ' ';  -- error
ALTER TAG tag4 ADD ALLOWED_VALUES '';
ALTER TAG tag4 ADD ALLOWED_VALUES '';   -- error

SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

-- DROP allowed_values
ALTER TAG tag1 DROP ALLOWED_VALUES 'unknown';    -- error;
ALTER TAG IF EXISTS tag1 DROP ALLOWED_VALUES 'unknown';    -- error;
ALTER TAG IF EXISTS tag1 DROP ALLOWED_VALUES '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',    -- error
 '17',  '18',  '19',  '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30',  '31',  '32',  '33',  '34',  '35',  '36',
 '37',  '38',  '39',  '40',  '41',  '42',  '43',  '44',  '45',  '46',  '47',  '48',  '49',  '50',  '51',  '52',  '53',  '54',  '55',  '56',
 '57',  '58',  '59',  '60',  '61',  '62',  '63',  '64',  '65',  '66',  '67',  '68',  '69',  '70',  '71',  '72',  '73',  '74',  '75',  '76',
 '77',  '78',  '79',  '80',  '81',  '82',  '83',  '84',  '85',  '86',  '87',  '88',  '89',  '90',  '91',  '92',  '93',  '94',  '95',  '96',
 '97',  '98',  '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116',
'117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136',
'137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156',
'157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176',
'177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196',
'197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216',
'217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236',
'237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255', '256',
'257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274', '275', '276',
'277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287', '288', '289', '290', '291', '292', '293', '294', '295', '296',
'297', '298', '299', '300';
ALTER TAG tag1 DROP ALLOWED_VALUES '1'; -- error
ALTER TAG IF EXISTS tag1 DROP ALLOWED_VALUES '1'; -- error
ALTER TAG tag1 DROP ALLOWED_VALUES '';  -- error
ALTER TAG IF EXISTS tag1 DROP ALLOWED_VALUES '';  -- error
ALTER TAG tag1 UNSET ALLOWED_VALUES;
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;

ALTER TAG tag1 ADD ALLOWED_VALUES 'val1', 'val2', 'val3';
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c other_db
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;
SELECT count(*) FROM pg_tag_description;
\c postgres

-- Cleanup
DROP USER tag_user;

-- Test database with tag
-- Create database
CREATE DATABASE tag_db1 TAG (tag1 = 'novalue'); -- error
CREATE DATABASE tag_db1 TAG (tag1 = 'val1');
CREATE DATABASE tag_db2 TAG;    -- error
CREATE DATABASE tag_db2 TAG (); -- error
CREATE DATABASE tag_db2 TAG (tag1 = '');    -- error
CREATE DATABASE tag_db2 TAG (tag1 = 'val1', tag2 = '10');
CREATE DATABASE tag_db3 TAG (tag1 = 'val1', tag1 = 'val2', tag1 = 'val1');    -- error
CREATE DATABASE tag_db3 TAG (tag1 = 'val1', tag1 = 'novalue');  -- error
CREATE DATABASE tag_db3 TAG (tag1 = 'val1', tag2 = '1');
CREATE DATABASE tag_db4 TAG (tag1 = 'val1', tag2 = '3', tag4 = '');
CREATE DATABASE tag_db5 TAG (tag2 = '300', tag4 = ' ');
CREATE DATABASE tag_db6 TAG (tag4 = '', tag4 = ' '); -- error
CREATE DATABASE tag_db6 TAG (tag4 = '', tag4 = ''); -- error
CREATE DATABASE tag_db6 TAG (tag4 = ' ', tag4 = ' ');   -- error
CREATE DATABASE tag_db6 TAG (tag4 = '');
CREATE DATABASE tag_db7 TAG (tag4 = ' ');
CREATE DATABASE tag_db8 TAG (tag3 = '');    -- error
CREATE DATABASE tag_db8 WITH TEMPLATE = template0 TAG (tag1 = 'novalue');   -- error
CREATE DATABASE tag_db8 WITH TEMPLATE = template0 TAG (tag1 = 'val1');
CREATE DATABASE tag_db9 WITH TEMPLATE = template0;
CREATE DATABASE tag_db10 WITH TEMPLATE = template0 TAG (tag3 = 'dadkqjefpqfqfqe4l123j9i1snkqenp3412n4jnflqjenfaddpiqepj21304i12;kfnqpqnepfqefqwef');

SELECT * FROM database_tag_descriptions
ORDER BY 1, 2, 3, 4;

SELECT datname, datdba, encoding, datistemplate, datallowconn, datconnlimit
FROM pg_database
WHERE datname like '%tag%' ORDER BY 1;

DROP TAG tag1;  -- error
DROP TAG tag2;  -- error
DROP TAG tag3;  -- error
DROP TAG tag4;  -- error
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;

-- Alter Database
ALTER DATABASE tag_db1 TAG (tag1 = 'val1', tag1 = 'val1');
ALTER DATABASE tag_db2 TAG (tag2 = '10');
ALTER DATABASE tag_db3 TAG (tag3 = ''); -- error
ALTER DATABASE tag_db4 UNSET TAG (tag1, tag2);
ALTER DATABASE tag_db4 TAG (tag1 = 'val3', tag4 = '');
ALTER DATABASE tag_db5 TAG (tag4 = '', tag4 = ' ');
ALTER DATABASE tag_db6 TAG (tag4 = ' '); 
ALTER DATABASE tag_db7 TAG (tag1 = 'val2', tag2 = '3');
ALTER DATABASE tag_db7 UNSET TAG (tag1);
ALTER DATABASE tag_db8 TAG (tag4 = '123', tag4 = '123');
ALTER DATABASE tag_db9 TAG (tag4 = '', tag4 = '123', tag4 = '456');
ALTER DATABASE tag_db9 TAG (tag4 = '123');

SELECT * FROM database_tag_descriptions
ORDER BY 1, 2, 3, 4;

SELECT datname, datdba, encoding, datistemplate, datallowconn, datconnlimit
FROM pg_database
WHERE datname like '%tag%' ORDER BY 1;

-- Drop Database
DROP DATABASE tag_db1;
DROP DATABASE tag_db2;
DROP DATABASE tag_db3;
DROP DATABASE tag_db4;
DROP DATABASE tag_db5;
DROP DATABASE tag_db6;
DROP DATABASE tag_db7;
DROP DATABASE tag_db8;
DROP DATABASE tag_db9;
DROP DATABASE tag_db10;

SELECT * FROM database_tag_descriptions
ORDER BY 1, 2, 3, 4;

SELECT datname, datdba, encoding, datistemplate, datallowconn, datconnlimit
FROM pg_database
WHERE datname like '%tag%' ORDER BY 1;


-- Test user with tag
-- Create User
CREATE USER tag_user1 TAG (tag1 = 'novalue'); -- error
CREATE USER tag_user1 TAG (tag1 = 'val1');
CREATE USER tag_user2 TAG;    -- error
CREATE USER tag_user2 TAG (); -- error
CREATE USER tag_user2 TAG (tag1 = '');    -- error
CREATE USER tag_user2 TAG (tag1 = 'val1', tag2 = '10');
CREATE USER tag_user3 TAG (tag1 = 'val1', tag1 = 'val2', tag1 = 'val1');    -- error
CREATE USER tag_user3 TAG (tag1 = 'val1', tag1 = 'novalue');  -- error
CREATE USER tag_user3 TAG (tag1 = 'val1', tag2 = '1');
CREATE USER tag_user4 TAG (tag1 = 'val1', tag2 = '3', tag4 = '');
CREATE USER tag_user5 TAG (tag2 = '300', tag4 = ' ');
CREATE USER tag_user6 TAG (tag4 = '', tag4 = ''); -- error
CREATE USER tag_user6 TAG (tag4 = ' ', tag4 = ' ');   -- error
CREATE USER tag_user6 TAG (tag4 = '');
CREATE USER tag_user7 CONNECTION LIMIT 10 TAG (tag4 = ' ');
CREATE USER tag_user8 TAG (tag3 = '');    -- error
CREATE USER tag_user8 superuser TAG (tag1 = 'novalue');   -- error
CREATE USER tag_user8 superuser TAG (tag1 = 'val1');
CREATE USER tag_user9 superuser;
CREATE USER tag_user10 superuser TAG (tag3 = 'dadkqjefpqfqfqe4l123j9i1snkqenp3412n4jnflqjenfaddpiqepj21304i12;kfnqpqnepfqefqwef');

SELECT * FROM user_tag_descriptions
WHERE rolname like '%tag_user%'
ORDER BY 1, 2, 3, 4;

SELECT rolname, rolsuper, rolconnlimit FROM pg_authid
WHERE rolname like '%tag_user%'
ORDER BY 1;

DROP TAG tag1;  -- error
DROP TAG tag2;  -- error
DROP TAG tag3;  -- error
DROP TAG tag4;  -- error
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;

-- Alter User
ALTER USER tag_user1 CONNECTION LIMIT 3 TAG (tag1 = 'val1', tag1 = 'val1'); -- error
ALTER USER tag_user1 TAG (tag1 = 'val1', tag1 = 'val1');
ALTER USER tag_user2 TAG (tag2 = '10');
ALTER USER tag_user3 TAG (tag3 = ''); -- error
ALTER USER tag_user4 UNSET TAG (tag1, tag2);
ALTER USER tag_user4 TAG (tag1 = 'val3', tag4 = '');
ALTER USER tag_user5 TAG (tag4 = '', tag4 = ' ');
ALTER USER tag_user6 TAG (tag4 = ' '); 
ALTER USER tag_user7 TAG (tag1 = 'val2', tag2 = '3');
ALTER USER tag_user7 UNSET TAG (tag1);
ALTER USER tag_user8 TAG (tag4 = '123', tag4 = '123');
ALTER USER tag_user9 TAG (tag4 = '', tag4 = '123', tag4 = '456');
ALTER USER tag_user9 TAG (tag4 = '123');

SELECT * FROM user_tag_descriptions
WHERE rolname like '%tag_user%'
ORDER BY 1, 2, 3, 4;

SELECT rolname, rolsuper, rolconnlimit FROM pg_authid
WHERE rolname like '%tag_user%'
ORDER BY 1;

-- Drop User
DROP USER tag_user1;
DROP USER tag_user2;
DROP USER tag_user3;
DROP USER tag_user4;
DROP USER tag_user5;
DROP USER tag_user6;
DROP USER tag_user7;
DROP USER tag_user8;
DROP USER tag_user9;
DROP USER tag_user10;

SELECT * FROM user_tag_descriptions
WHERE rolname like '%tag_user%'
ORDER BY 1, 2, 3, 4;

SELECT rolname, rolsuper, rolconnlimit FROM pg_authid
WHERE rolname like '%tag_user%'
ORDER BY 1;


-- Test tablespace with tag
-- Create tablespace
CREATE TABLESPACE tag_tablespace LOCATION '@testtablespace@' WITH (random_page_cost = 3.0) TAG (tag1 = 'test'); -- error
CREATE TABLESPACE tag_tablespace LOCATION '@testtablespace@' WITH (random_page_cost = 3.0) TAG (tag1 = 'val1', tag2 = '301');   -- error
CREATE TABLESPACE tag_tablespace LOCATION '@testtablespace@' WITH (random_page_cost = 3.0) TAG (tag1 = 'val1', tag2 = '2');

SELECT * FROM tablespace_tag_descriptions
ORDER BY 1, 2, 3, 4;

SELECT spcname FROM pg_tablespace
ORDER BY 1;

-- Alter tablespace
ALTER TABLESPACE tag_tablespace SET (random_page_cost = 1.0, seq_page_cost = 1.1) TAG (tag2 = '10');    -- error
ALTER TABLESPACE tag_tablespace SET (random_page_cost = 1.0, seq_page_cost = 1.1) TAG (tag3 = ''); -- error
ALTER TABLESPACE tag_tablespace SET (random_page_cost = 1.0, seq_page_cost = 1.1) TAG (tag4 = '', tag1 = 'val1');   -- error
ALTER TABLESPACE tag_tablespace TAG (tag4 = '', tag1 = 'val1');
ALTER TABLESPACE tag_tablespace UNSET TAG (tag1, tag2);
ALTER TABLESPACE tag_tablespace UNSET TAG (tag2);
ALTER TABLESPACE tag_tablespace TAG (tag2 = '10');
ALTER TABLESPACE tag_tablespace TAG (tag1 = 'val2', tag1 = 'val3');

SELECT * FROM tablespace_tag_descriptions
ORDER BY 1, 2, 3, 4;

SELECT spcname FROM pg_tablespace
ORDER BY 1;

DROP TAG tag1;  -- error
DROP TAG tag2;  -- error
DROP TAG tag4;  -- error
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;

-- Drop tablespace
DROP TABLESPACE tag_tablespace;

SELECT * FROM tablespace_tag_descriptions
ORDER BY 1, 2, 3, 4;

SELECT spcname FROM pg_tablespace
ORDER BY 1;


-- Test Schema with Tag
-- Create Schema
CREATE SCHEMA tag_schema1 TAG (tag1 = 'val1');  -- error
CREATE SCHEMA tag_schema1 WITH TAG (tag1 = 'novalue'); -- error
CREATE SCHEMA tag_schema1 WITH TAG (tag1 = 'val1');
CREATE SCHEMA tag_schema2 WITH TAG;    -- error
CREATE SCHEMA tag_schema2 WITH TAG (); -- error
CREATE SCHEMA tag_schema2 WITH TAG (tag1 = '');    -- error
CREATE SCHEMA tag_schema2 WITH TAG (tag1 = 'val1', tag2 = '10');
CREATE SCHEMA tag_schema3 WITH TAG (tag1 = 'val1', tag1 = 'val2', tag1 = 'val1');    -- error
CREATE SCHEMA tag_schema3 WITH TAG (tag1 = 'val1', tag1 = 'novalue');  -- error
CREATE SCHEMA tag_schema3 WITH TAG (tag1 = 'val1', tag2 = '1');
CREATE SCHEMA tag_schema4 WITH TAG (tag1 = 'val1', tag2 = '3', tag4 = '');
CREATE SCHEMA tag_schema5 WITH TAG (tag2 = '300', tag4 = ' ');
CREATE SCHEMA tag_schema6 WITH TAG (tag4 = '', tag4 = ''); -- error
CREATE SCHEMA tag_schema6 WITH TAG (tag4 = ' ', tag4 = ' ');   -- error
CREATE SCHEMA tag_schema6 WITH TAG (tag4 = '');
CREATE SCHEMA tag_schema7 WITH TAG (tag4 = ' ');
CREATE SCHEMA tag_schema8 WITH TAG (tag3 = '');    -- error
CREATE SCHEMA tag_schema8 WITH TAG (tag1 = 'novalue');   -- error
CREATE SCHEMA tag_schema8 WITH TAG (tag1 = 'val1');
CREATE SCHEMA tag_schema9;
CREATE SCHEMA tag_schema10 WITH TAG (tag3 = 'dadkqjefpqfqfqe4l123j9i1snkqenp3412n4jnflqjenfaddpiqepj21304i12;kfnqpqnepfqefqwef');

SELECT * FROM schema_tag_descriptions
WHERE nspname like '%tag_schema%'
ORDER BY 1, 2, 3, 4;

SELECT nspname, nspowner FROM pg_namespace
WHERE nspname like '%tag_schema%'
ORDER BY 1;

DROP TAG tag1;  -- error
DROP TAG tag2;  -- error
DROP TAG tag3;  -- error
DROP TAG tag4;  -- error
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;

-- Alter Schema
ALTER SCHEMA tag_schema1 TAG (tag1 = 'val1', tag1 = 'val1');
ALTER SCHEMA tag_schema2 TAG (tag2 = '10');
ALTER SCHEMA tag_schema3 TAG (tag3 = ''); -- error
ALTER SCHEMA tag_schema4 UNSET TAG (tag1, tag2);
ALTER SCHEMA tag_schema4 TAG (tag1 = 'val3', tag4 = '');
ALTER SCHEMA tag_schema5 TAG (tag4 = '', tag4 = ' ');
ALTER SCHEMA tag_schema6 TAG (tag4 = ' '); 
ALTER SCHEMA tag_schema7 TAG (tag1 = 'val2', tag2 = '3');
ALTER SCHEMA tag_schema7 UNSET TAG (tag1);
ALTER SCHEMA tag_schema8 TAG (tag4 = '123', tag4 = '123');
ALTER SCHEMA tag_schema9 TAG (tag4 = '', tag4 = '123', tag4 = '456');
ALTER SCHEMA tag_schema9 TAG (tag4 = '123');

SELECT * FROM schema_tag_descriptions
WHERE nspname like '%tag_schema%'
ORDER BY 1, 2, 3, 4;

SELECT nspname, nspowner FROM pg_namespace
WHERE nspname like '%tag_schema%'
ORDER BY 1;

-- Drop Schema
DROP SCHEMA tag_schema1;
DROP SCHEMA tag_schema2;
DROP SCHEMA tag_schema3;
DROP SCHEMA tag_schema4;
DROP SCHEMA tag_schema5;
DROP SCHEMA tag_schema6;
DROP SCHEMA tag_schema7;
DROP SCHEMA tag_schema8;
DROP SCHEMA tag_schema9;
DROP SCHEMA tag_schema10;

SELECT * FROM schema_tag_descriptions
WHERE nspname like '%tag_schema%'
ORDER BY 1, 2, 3, 4;

SELECT nspname, nspowner FROM pg_namespace
WHERE nspname like '%tag_schema%'
ORDER BY 1;


-- Test Table with Tag
-- Create Table
CREATE TABLE tag_table1(a int, b varchar) TAG (tag1 = 'novalue'); -- error
CREATE TEMP TABLE tag_table1(a int, b varchar) TAG (tag1 = 'val1');
CREATE TEMP TABLE tag_table2(a int, b varchar) TAG;    -- error
CREATE TEMP TABLE tag_table2(a int, b varchar) TAG (); -- error
CREATE TABLE tag_table2(a int, b varchar) TAG (tag1 = '');    -- error
CREATE TEMP TABLE tag_table2(a int, b varchar) TAG (tag1 = 'val1', tag2 = '10');
CREATE TABLE tag_table3(a int, b varchar) TAG (tag1 = 'val1', tag1 = 'val2', tag1 = 'val1');    -- error
CREATE TABLE tag_table3(a int, b varchar) TAG (tag1 = 'val1', tag1 = 'novalue');  -- error
CREATE TEMP TABLE tag_table3(a int, b varchar) TAG (tag1 = 'val1', tag2 = '1');
CREATE TABLE tag_table4(a int, b varchar) TAG (tag1 = 'val1', tag2 = '3', tag4 = '');
CREATE TABLE tag_table5(a int, b varchar) TAG (tag2 = '300', tag4 = ' ');
CREATE TABLE tag_table6(a int, b varchar) TAG (tag4 = '', tag4 = ''); -- error
CREATE TABLE tag_table6(a int, b varchar) TAG (tag4 = ' ', tag4 = ' ');   -- error
CREATE TABLE tag_table6(a int, b varchar) TAG (tag4 = '');
CREATE TABLE tag_table7(a int, b varchar) TAG (tag4 = ' ');
CREATE TABLE tag_table8(a int, b varchar) TAG (tag3 = '');    -- error
CREATE TABLE tag_table8(a int, b varchar) TAG (tag1 = 'novalue');   -- error
CREATE TABLE tag_table8(a int, b varchar) TAG (tag1 = 'val1');
CREATE TABLE tag_table9(a int, b varchar);
CREATE TABLE tag_table10(a int, b varchar) TAG (tag3 = 'dadkqjefpqfqfqe4l123j9i1snkqenp3412n4jnflqjenfaddpiqepj21304i12;kfnqpqnepfqefqwef');

SELECT * FROM relation_tag_descriptions
WHERE relname like '%tag_table%'
ORDER BY 1, 2, 3, 4, 5, 6;

SELECT relname, relpersistence FROM pg_class
WHERE relname like '%tag_table%'
ORDER BY 1;

DROP TAG tag1;  -- error
DROP TAG tag2;  -- error
DROP TAG tag3;  -- error
DROP TAG tag4;  -- error
SELECT tagname, tagowner, allowed_values FROM pg_tag ORDER BY 1;

-- Alter Table
ALTER TABLE tag_table1 TAG (tag1 = 'val1', tag1 = 'val1');
ALTER TABLE tag_table2 TAG (tag2 = '10');
ALTER TABLE tag_table3 TAG (tag3 = '');
ALTER TABLE tag_table4 UNSET TAG (tag1, tag2);
ALTER TABLE tag_table4 TAG (tag1 = 'val3', tag4 = '');
ALTER TABLE tag_table5 TAG (tag4 = '', tag4 = ' ');
ALTER TABLE tag_table6 TAG (tag4 = ' '); 
ALTER TABLE tag_table7 TAG (tag1 = 'val2', tag2 = '3');
ALTER TABLE tag_table7 UNSET TAG (tag1);
ALTER TABLE tag_table8 TAG (tag4 = '123', tag4 = '123');
ALTER TABLE tag_table9 TAG (tag4 = '', tag4 = '123', tag4 = '456');
ALTER TABLE tag_table9 TAG (tag4 = '123');

SELECT * FROM relation_tag_descriptions
WHERE relname like '%tag_table%'
ORDER BY 1, 2, 3, 4, 5, 6;

SELECT relname, relpersistence FROM pg_class
WHERE relname like '%tag_table%'
ORDER BY 1;

-- Drop Table
DROP TABLE tag_table1;
DROP TABLE tag_table2;
DROP TABLE tag_table3;
DROP TABLE tag_table4;
DROP TABLE tag_table5;
DROP TABLE tag_table6;
DROP TABLE tag_table7;
DROP TABLE tag_table8;
DROP TABLE tag_table9;
DROP TABLE tag_table10;

SELECT * FROM relation_tag_descriptions
WHERE relname like '%tag_table%'
ORDER BY 1, 2, 3, 4, 5, 6;

SELECT relname, relpersistence FROM pg_class
WHERE relname like '%tag_table%'
ORDER BY 1;

-- Test tag with null allowed_values
CREATE TAG tag_with_null_values;
CREATE DATABASE test TAG (tag_with_null_values = '123');
ALTER DATABASE test TAG (tag_with_null_values = 'qweda');
DROP TAG tag_with_null_values;	-- error

DROP DATABASE test;

-- Cleanup
DROP TAG tag1;
DROP TAG tag2;
DROP TAG tag3;
DROP TAG tag4;
DROP TAG tag_with_null_values;
DROP DATABASE other_db;
