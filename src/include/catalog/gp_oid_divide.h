/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * oid_divide.h
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/oid_divide.h
 *
 * NOTES
 * 		This is used to divide oid range for core and extensions
 *  	to avoid duplicated.
 *
 *-------------------------------------------------------------------------
 */

#ifndef GP_OID_DIVIDE_H
#define GP_OID_DIVIDE_H

/*
 * Extensions should use Oids start from EXT_OID_START!
 *
 * To avoid duplicated oids across extensions or repos.
 * We strongly suggest extesions should begin from EXT_OID_START
 * to separate kernel and extensions.
 */
#define EXT_OID_START 9932

#endif			/* GP_OID_DIVIDE_H */
