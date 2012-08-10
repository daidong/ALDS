/**
 * MemCached Java Client Logger
 * Copyright (c) 2007 Greg Whalin
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the BSD license
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.
 *
 * You should have received a copy of the BSD License along with this
 * library.
 *
 * @author Greg Whalin <greg@meetup.com> 
 * @version 2.0
 */

/*******************************************************************************
 * Copyright (c) 2009 Schooner Information Technology, Inc.
 * All rights reserved.
 * 
 * http://www.schoonerinfotech.com/
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.danga.MemCached;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a generic logger class for use in logging.
 * 
 * This can easily be swapped out for any other logging package in the main
 * code. For now, this is just a quick and dirty logger which will allow you to
 * specify log levels, but only wraps system.out.println.
 * 
 * @author Greg Whalin <greg@meetup.com>
 * @version 1.5
 */
public class Logger {

	public static final int LEVEL_DEBUG = 0;
	public static final int LEVEL_INFO = 1;
	public static final int LEVEL_WARN = 2;
	public static final int LEVEL_ERROR = 3;
	public static final int LEVEL_FATAL = 4;

	private static Map<String, Logger> loggers = new HashMap<String, Logger>();

	private String name;
	private int level;

	public void setLevel(int level) {
		this.level = level;
	}

	public final int getLevel() {
		return this.level;
	}

	protected Logger(String name, int level) {
		this.name = name;
		this.level = level;
	}

	protected Logger(String name) {
		this(name, LEVEL_INFO);
	}

	/**
	 * Gets a Logger obj for given name and level.
	 * 
	 * @param name
	 * @param level
	 * @return
	 */
	public static synchronized Logger getLogger(String name, int level) {
		Logger log = getLogger(name);
		if (log.getLevel() != level)
			log.setLevel(level);
		return log;
	}

	/**
	 * Gets a Logger obj for given name and sets default level.
	 * 
	 * @param name
	 * @return
	 */
	public static synchronized Logger getLogger(String name) {

		Logger log = null;
		if (loggers.containsKey(name)) {
			log = loggers.get(name);
		} else {
			log = new Logger(name);
			loggers.put(name, log);
		}

		return log;
	}

	/**
	 * logs mesg to std out and prints stack trace if exception passed in
	 * 
	 * @param mesg
	 * @param ex
	 */
	private void log(String mesg, Throwable ex) {
		System.out.println(name + " " + new Date() + " - " + mesg);
		if (ex != null)
			ex.printStackTrace(System.out);
	}

	/**
	 * logs a debug mesg
	 * 
	 * @param mesg
	 * @param ex
	 */
	public void debug(String mesg, Throwable ex) {
		if (this.level > LEVEL_DEBUG)
			return;

		log(mesg, ex);
	}

	public void debug(String mesg) {
		debug(mesg, null);
	}

	public final boolean isDebugEnabled() {
		return this.level <= LEVEL_DEBUG;
	}

	/**
	 * logs info mesg
	 * 
	 * @param mesg
	 * @param ex
	 */
	public void info(String mesg, Throwable ex) {
		if (this.level > LEVEL_INFO)
			return;

		log(mesg, ex);
	}

	public void info(String mesg) {
		info(mesg, null);
	}

	public final boolean isInfoEnabled() {
		return this.level <= LEVEL_INFO;
	}

	/**
	 * logs warn mesg
	 * 
	 * @param mesg
	 * @param ex
	 */
	public void warn(String mesg, Throwable ex) {
		if (this.level > LEVEL_WARN)
			return;

		log(mesg, ex);
	}

	public void warn(String mesg) {
		warn(mesg, null);
	}

	/**
	 * logs error mesg
	 * 
	 * @param mesg
	 * @param ex
	 */
	public void error(String mesg, Throwable ex) {
		if (this.level > LEVEL_ERROR)
			return;

		log(mesg, ex);
	}

	public void error(String mesg) {
		error(mesg, null);
	}

	/**
	 * logs fatal mesg
	 * 
	 * @param mesg
	 * @param ex
	 */
	public void fatal(String mesg, Throwable ex) {
		if (this.level > LEVEL_FATAL)
			return;

		log(mesg, ex);
	}

	public void fatal(String mesg) {
		fatal(mesg, null);
	}
}