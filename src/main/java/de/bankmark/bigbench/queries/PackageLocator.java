/*******************************************************************************
 * Copyright (c) 2014 bankmark UG 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.bankmark.bigbench.queries;

import java.io.InputStream;
import java.net.URL;

/**
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 27.04.2014
 */
public class PackageLocator {
	public static final String ABSOULUTE_RESOURCE_PATH = "/"
			+ PackageLocator.class.getPackage().getName().replace('.', '/')
			+ "/";

	public static InputStream getPackageResourceAsStream(String name) {
		return PackageLocator.class.getResourceAsStream(name);
	}

	public static URL getPackageResource(String name) {
		return PackageLocator.class.getResource(name);
	}
}
