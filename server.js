import http from 'http';
import fs from 'fs';
import path, { dirname } from 'path';
import { fileURLToPath } from 'url';
import {v4 as uuid4} from 'uuid';

const __dirname = dirname(fileURLToPath(import.meta.url));

const filesFolder = `${__dirname}\\files`;

/* расчширения и типы контента, поддерживающие частичный возврат */
const rangesContentTypes = {
	'.mp4': 'audio/mp4',
	'.txt':  'text',
	'.html': 'text/html; charset=UTF-8'
};

const ERRORS = {
	NOT_FOUND: {statusCode: 404, statusMessage: 'Not Found'},
	METHOD_NOT_ALLOWED: {statusCode: 405, statusMessage: 'Method Not Allowed'},
	BYTES_UNIT_NOT_VALID: {statusCode: 416, statusMessage: 'Bytes unit not valid'},
	RANGE_NOT_SATISFABLE: {statusCode: 416, statusMessage: 'Range Not Satisfable'},
	INTERNAL_SERVER_ERROR: {statusCode: 500, statusMessage: 'Internal Server Error'}
}

/* размер буфера чтения */
const highWaterMark = 65536; // 64 kB
/* ограничение на длину возвращаемого куска */
const partialMaxLength = 524288; // 512 kB

http.createServer(httpListener).listen(8000);

function httpListener(request, response) {
	try {
		/* доступно только методы GET и HEAD */
		if (!['GET', 'HEAD'].includes(request.method)) {
			throw ERRORS.METHOD_NOT_ALLOWED;
		}

		const filePath = `${filesFolder}${request.url.split('/').join(path.sep)}`

		/* проверяем есть ли соответствующий файл */
		if (!fs.existsSync(filePath)) {
			throw ERRORS.NOT_FOUND;
		}

		const fileName = path.basename(filePath);
		const fileExtension = path.extname(filePath);
		const contentInfo = getContentInfo(fileExtension);
		const fileSize = fs.statSync(filePath).size;

		response.statusCode = 200;
		response.statusMessage = 'OK';
		response.setHeader('Content-Type', contentInfo.type);
		response.setHeader('Content-Length', fileSize);
		response.setHeader('Cache-Control', 'no-cache');
	
		/* поддерживается ли для такого типа файлов возврат через запрос диапазона */
		if (contentInfo.rangeSupport) {
			/* черзе поле заголовка Accept-Ranges сервер показывает, что поддерживает запросы диапазона для целевого ресурса */
			response.setHeader('Accept-Ranges','bytes');
		}
		/* возврат через диапазон поддерживается и осуществляется запрос диапазона */
		if (contentInfo.rangeSupport && request.headers['range']) {
			/* запрашивается диапазон */
			/* надо выяснить какие части хочет клиент */
			response.statusCode = 206;
			const ranges = getRanges(request.headers['range'], fileSize, {partialMaxLength});
			if (ranges.byteRangeSet.length > 1) {
				/* запрашивается несколько диапазонов */
				const boundary = uuid4(); /* разделитель для частей */
				const partialHeaders = partialContentHeaders(ranges.byteRangeSet, boundary, contentInfo.type, fileSize);
				const endBoundary = `\r\n--${boundary}--`;

				response.setHeader('Content-Type',`multipart/byteranges; boundary=${boundary}`);
				response.setHeader('Content-Length',`${ranges.length + partialHeaders.size + endBoundary.length}`);

				if (request.method === 'HEAD') {
					response.end();
					return null;
				}

				let streams = [];
				ranges.byteRangeSet.forEach((value) => {
					streams.push(fs.createReadStream(filePath, {start: value.start, end: value.end, highWaterMark, emitClose: true}));
				});
				responseAllStream(streams, partialHeaders.headers, response).then(() => {
					response.end(endBoundary);
				}).catch((error) => {
					console.log(`Error in reading readable stream: "${error}"`);
					throw ERRORS.INTERNAL_SERVER_ERROR;
				})
			} else {
				/* запрашивается один диапазон */
				const {start, end} = ranges.byteRangeSet[0];
				response.setHeader('Content-Range',`bytes ${start}-${end}/${fileSize}`);
				
				if (request.method === 'HEAD') {
					response.end();
					return null;
				}

				const readStream = fs.createReadStream(filePath, {start, end, highWaterMark});
				readStream.pipe(response);
			}
		} else {
			/* диапазон не запрашивается или не поддерживается
			 * возвращаем файл целиком */
			response.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);

			if (request.method === 'HEAD') {
				response.end();
				return null;
			}

			const readStream = fs.createReadStream(filePath, {highWaterMark});
			readStream.pipe(response);
		}
	} catch (error) {
		console.log(`Internal Server Error: ${JSON.stringify(error)}`);
		response.statusCode = error.statusCode || 500;
		response.statusMessage = error.statusMessage || 'Internal Server Error';
		switch(error.statusCode) {
			case ERRORS.RANGE_NOT_SATISFABLE.statusCode:
				response.setHeader('Content-Range', `bytes */${error.fileSize}`);
				break;
			case ERRORS.METHOD_NOT_ALLOWED.statusCode:
				response.setHeader('Allow', 'GET,HEAD');
				break;
		}
		response.end();
	}
	return null;
}

function getContentInfo(extension) {
	let type = rangesContentTypes[extension];
	let rangeSupport = true;
	if (!type) {
		type = 'application/octet-stream';
		rangeSupport = false;
	}
	return {rangeSupport, type};
}

function partialContentHeaders(byteRangeSet, boundary, contentType, fileSize) {
	let headers = [];
	let size = 0;
	byteRangeSet.forEach((range, index) => {
		let header = '';
		header+=`${index ? '\r\n' : ''}--${boundary}\r\n`;
		header+=`Content-Type: ${contentType}\r\n`;
		header+=`Content-Range: bytes ${range.start}-${range.end}/${fileSize}\r\n`
		header+=`\r\n`;
		headers.push(header);
		size += header.length;
	})
	return {headers, size}
}

function responseAllStream(streams, headers, response) {
	/* читаем всё по очереди */
	return new Promise((resolve, reject) => {
		const readableStream = streams.shift();
		const paritalHeader = headers.shift();
		if (readableStream) {
			response.write(paritalHeader);
			readableStream.pipe(response, {end: false});
			readableStream.on('close', () => {
				resolve('Stream is closed');
			})
			readableStream.on('end', () => {
				resolve('Stream is ended');
			})
			readableStream.on('error', (error) => {
				reject(error);
			});
		} else {
			/* нет потока для чтения */
			resolve();
		}
	}).then((result) => {
		/* предыдущий поток дочитан */
		if (streams.length > 0) {
			/* читаем/печатаем следующий фрагмент */
			return responseAllStream(streams, headers, response);
		}
		return result;
	});
}

function getRanges(range, fileSize, {correctValues = false, preventAtack = true, partialMaxLength = 0} = {}) {
	/* bytes=0-499 
	   bytes=-100
	   bytes=100-
	   bytes=0-0,-1
	   bytes=500-600,601-999 */
	const [bytesUnit, byteRangeSetString] = range.trim().split('=');
	if (bytesUnit !== 'bytes') {
		throw ERRORS.BYTES_UNIT_NOT_VALID;
	}
	const byteRangeSet = []
	let length = 0;
	/* получем диапазоны */
	byteRangeSetString.split(',').forEach((value, index) => {
		let [, start, end] = /([0-9]{0,})\-([0-9]{0,})/g.exec(value);
		start = start ? Number(start) : start;
		end = end ? Number(end) : end;
		/* не задано стартовое значение, запрашивается с конца */
		if (start !== 0 && !start) {
			start = fileSize - end;
			end = fileSize - 1;
		}
		/* не задано конечное значение, запрашивается со старта до конца */
		if (end !== 0 && !end) {
			end = fileSize - 1;
		}
		/* корректируем значения [0, fileSize] если установлен параметр "correctValues" */
		const limit = (val) => correctValues ? (val < 0 ? 0 : (val > fileSize - 1 ? fileSize - 1 : val)) : val; 
		start = limit(start);
		end = limit(end);
		/* если задана опция "partialMaxLength" - ограничиваем длину возвращаемого контента */
		if (partialMaxLength && (end - start + 1) > partialMaxLength) {
			end = start + partialMaxLength - 1;
		}
		length += end - start + 1;
		byteRangeSet.push({start, end});
	})
	/* если задана опция "preventAtack" - предотвращаем атаку "отказ в обслуживании" */
	if (preventAtack && isDenialOfServiceAttacks(byteRangeSet)) {
		throw {...ERRORS.RANGE_NOT_SATISFABLE, fileSize};
	}
	
	return {byteRangeSet, length} ;
}

function isDenialOfServiceAttacks(byteRangeSet) {
	/* Предотвращаем атаку типа "отказ в обслуживании", для простоты запрос набрал больше POINT_OF_NOT_SATISFABLE очков - возвращаем код 416 */
	const POINT_OF_NOT_SATISFABLE = 10;
	/* отказываем если производится попытка запросить пересекающиес диапазона (POINT_CROSS_RANGE очка к отказу )*/
	const POINT_CROSS_RANGE = 3;
	/* много небольших диапазонов в одном наборе (POINT_SMALL_RANGE очка за небольшой диапазон) */
	const POINT_SMALL_RANGE = 2;
	const SMALL_RANGE = 65536; // 64 kB
	/* особенно когда диапазоны запрашиваются не по порядку (х1.5 очков к отказу, изначально мультипликатор 1)*/
	const POINT_NOT_SORTED_MULTIPLICATOR = 1.5;

	let POINT_SUM = 0;
	let MULTIPLICATOR = 1;
	let check = byteRangeSet[0];
	byteRangeSet.forEach((value, index) => {
		/* начало диапазона больше конца, значит сразу всё плохо */
		if (value.start > value.end) {
			POINT_SUM += POINT_OF_NOT_SATISFABLE;
		}
		/* запрашивается маленький отрезок */
		if (value.end - value.start < SMALL_RANGE) {
			POINT_SUM += POINT_SMALL_RANGE;
		}
		/* если это не первый элемент смотрим чтобы элементы шли по возрастанию и не пересекались */
		if (index > 0) {
			if (check.end < value.start) {
				MULTIPLICATOR = POINT_NOT_SORTED_MULTIPLICATOR;
			}
			if ((Math.max(check.end, value.end) - Math.min(check.start, value.start)) < ((check.end - check.start) + (value.end - value.start))) {
				POINT_SUM += POINT_CROSS_RANGE;
			}
		}	
		check = value;
	})
	return (POINT_SUM * MULTIPLICATOR) >= POINT_OF_NOT_SATISFABLE;
}
