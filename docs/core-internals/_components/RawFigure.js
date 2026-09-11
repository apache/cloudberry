/*
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
 */

import React from 'react';
import './RawFigure.css';

/**
 * Renders a figure that is laid out with HTML and CSS rather than drawn as SVG.
 *
 * The markup arrives as a string and is injected, instead of being written as
 * JSX in the page. That is deliberate: these figures carry inline style
 * attributes, void tags and — in two cases — unbalanced tags, none of which
 * MDX will accept as JSX. Passing the markup as a string keeps MDX out of it
 * and leaves the rendering to the browser, so the figure looks the way it
 * looks in the standalone edition of the handbook.
 */
export default function RawFigure({html, caption}) {
  return (
    <figure className="book-figure">
      <div dangerouslySetInnerHTML={{__html: html}} />
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}
