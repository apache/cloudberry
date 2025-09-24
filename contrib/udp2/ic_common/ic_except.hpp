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
 * ic_except.hpp
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/ic_except.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_EXCEPT_HPP
#define IC_EXCEPT_HPP

class ICException: public std::runtime_error {
public:
	ICException(const std::string & arg, const char *file, int line): std::runtime_error(arg) {
		std::stringstream ss;
		ss << arg << " from " << file << ":" << line << std::endl;
		detail = ss.str();
	}
	~ICException() throw() {}

	virtual const char *msg() const {
		return detail.c_str();
	}

protected:
	std::string detail;
};

class ICFatalException: public ICException {
public:
	ICFatalException(const std::string & arg, const char *file, int line):
		ICException(arg, file, line) {}
	~ICFatalException() throw () {}
};

class ICInvalidIndex: public ICException {
public:
	ICInvalidIndex(const std::string & arg, const char *file, int line):
		ICException(arg, file, line) {}
	~ICInvalidIndex() throw () {}
};

class ICNetworkException: public ICException {
public:
	ICNetworkException(const std::string & arg, const char *file, int line):
		ICException(arg, file, line) {}
	~ICNetworkException() throw () {}
};

class ICReceiveThreadException: public ICException {
public:
	ICReceiveThreadException(const std::string & arg, const char *file, int line):
		ICException(arg, file, line) {}
	~ICReceiveThreadException() throw () {}
};

#endif // IC_EXCEPT_HPP