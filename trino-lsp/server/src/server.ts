/* --------------------------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 * ------------------------------------------------------------------------------------------ */
import {
	createConnection,
	TextDocuments,
	Diagnostic,
	DiagnosticSeverity,
	ProposedFeatures,
	InitializeParams,
	DidChangeConfigurationNotification,
	CompletionItem,
	CompletionItemKind,
	TextDocumentPositionParams,
	TextDocumentSyncKind,
	InitializeResult,
	Position,
	DocumentFormattingParams
} from 'vscode-languageserver/node';

import {
	TextDocument, TextEdit
} from 'vscode-languageserver-textdocument';
import fetch from 'node-fetch';

import {
	diff_match_patch as DiffMatchPatch,
	Diff,
	DIFF_DELETE,
	DIFF_INSERT,
	DIFF_EQUAL,
} from 'diff-match-patch';

// Create a connection for the server, using Node's IPC as a transport.
// Also include all preview / proposed LSP features.
let connection = createConnection(ProposedFeatures.all);

// Create a simple text document manager.
let documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);

let hasConfigurationCapability: boolean = false;
let hasWorkspaceFolderCapability: boolean = false;
let hasDiagnosticRelatedInformationCapability: boolean = false;

connection.onInitialize((params: InitializeParams) => {
	let capabilities = params.capabilities;

	// Does the client support the `workspace/configuration` request?
	// If not, we fall back using global settings.
	hasConfigurationCapability = !!(
		capabilities.workspace && !!capabilities.workspace.configuration
	);
	hasWorkspaceFolderCapability = !!(
		capabilities.workspace && !!capabilities.workspace.workspaceFolders
	);
	hasDiagnosticRelatedInformationCapability = !!(
		capabilities.textDocument &&
		capabilities.textDocument.publishDiagnostics &&
		capabilities.textDocument.publishDiagnostics.relatedInformation
	);

	const result: InitializeResult = {
		capabilities: {
			textDocumentSync: TextDocumentSyncKind.Incremental,
			// Tell the client that this server supports code completion.
			completionProvider: {
				resolveProvider: true
			},
			documentFormattingProvider: true
		}
	};
	if (hasWorkspaceFolderCapability) {
		result.capabilities.workspace = {
			workspaceFolders: {
				supported: true
			}
		};
	}
	return result;
});

connection.onInitialized(() => {
	if (hasConfigurationCapability) {
		// Register for all configuration changes.
		connection.client.register(DidChangeConfigurationNotification.type, undefined);
	}
	if (hasWorkspaceFolderCapability) {
		connection.workspace.onDidChangeWorkspaceFolders(_event => {
			connection.console.log('Workspace folder change event received.');
		});
	}
});

// The example settings
interface ExampleSettings {
	maxNumberOfProblems: number;
}

// The global settings, used when the `workspace/configuration` request is not supported by the client.
// Please note that this is not the case when using this server with the client provided in this example
// but could happen with other clients.
const defaultSettings: ExampleSettings = { maxNumberOfProblems: 1000 };
let globalSettings: ExampleSettings = defaultSettings;

// Cache the settings of all open documents
let documentSettings: Map<string, Thenable<ExampleSettings>> = new Map();

connection.onDidChangeConfiguration(change => {
	if (hasConfigurationCapability) {
		// Reset all cached document settings
		documentSettings.clear();
	} else {
		globalSettings = <ExampleSettings>(
			(change.settings.languageServerExample || defaultSettings)
		);
	}

	// Revalidate all open text documents
	documents.all().forEach(validateTextDocument);
});

function getDocumentSettings(resource: string): Thenable<ExampleSettings> {
	if (!hasConfigurationCapability) {
		return Promise.resolve(globalSettings);
	}
	let result = documentSettings.get(resource);
	if (!result) {
		result = connection.workspace.getConfiguration({
			scopeUri: resource,
			section: 'languageServerExample'
		});
		documentSettings.set(resource, result);
	}
	return result;
}

// Only keep settings for open documents
documents.onDidClose(e => {
	documentSettings.delete(e.document.uri);
});

// The content of a text document has changed. This event is emitted
// when the text document first opened or when its content has changed.
documents.onDidChangeContent(change => {
	validateTextDocument(change.document);
});

interface SqlParseResponse {
	formatted_sql: string
	suggestions: string[]
	parse_error: {
		message: string
		row: number
		column: number
	}
}

async function parseSql(sql: string): Promise<SqlParseResponse> {
	const response = await fetch('http://localhost:4567/v1/parse', {
        method: 'post',
        body:    JSON.stringify({ sql })
    });
	return await response.json();
}

async function validateTextDocument(textDocument: TextDocument): Promise<void> {
	// In this simple example we get the settings for every validate run.
	let settings = await getDocumentSettings(textDocument.uri);

	// The validator creates diagnostics for all uppercase words length 2 and more
	let text = textDocument.getText();

	const parseResponse = await parseSql(text);
	const parseError = parseResponse.parse_error;

	let diagnostics: Diagnostic[] = [];
	if (parseError) {
		let diagnostic: Diagnostic = {
			severity: DiagnosticSeverity.Error,
			range: {
				start: Position.create(parseError.row - 1, parseError.column),
				end: Position.create(parseError.row - 1, parseError.column)
			},
			message: parseError.message,
			source: 'Query Engine'
		};
		diagnostics.push(diagnostic);
	}

	// let pattern = /\b[A-Z]{2,}\b/g;
	// let m: RegExpExecArray | null;

	// let problems = 0;
	// let diagnostics: Diagnostic[] = [];
	// while ((m = pattern.exec(text)) && problems < settings.maxNumberOfProblems) {
	// 	problems++;
	// 	let diagnostic: Diagnostic = {
	// 		severity: DiagnosticSeverity.Warning,
	// 		range: {
	// 			start: textDocument.positionAt(m.index),
	// 			end: textDocument.positionAt(m.index + m[0].length)
	// 		},
	// 		message: `${m[0]} is all uppercase.`,
	// 		source: 'ex'
	// 	};
	// 	if (hasDiagnosticRelatedInformationCapability) {
	// 		diagnostic.relatedInformation = [
	// 			{
	// 				location: {
	// 					uri: textDocument.uri,
	// 					range: Object.assign({}, diagnostic.range)
	// 				},
	// 				message: 'Spelling matters'
	// 			},
	// 			{
	// 				location: {
	// 					uri: textDocument.uri,
	// 					range: Object.assign({}, diagnostic.range)
	// 				},
	// 				message: 'Particularly for names'
	// 			}
	// 		];
	// 	}
	// 	diagnostics.push(diagnostic);
	// }

	// Send the computed diagnostics to VSCode.
	connection.sendDiagnostics({ uri: textDocument.uri, diagnostics });
}

connection.onDidChangeWatchedFiles(_change => {
	// Monitored files have change in VSCode
	connection.console.log('We received an file change event');
});

// This handler provides the initial list of the completion items.
connection.onCompletion(
	async (params: TextDocumentPositionParams): Promise<CompletionItem[]> => {

		const { textDocument: ident } = params;
		const document = documents.get(ident.uri)!;

		const sql = document.getText();
		const parseResponse = await parseSql(sql);


		// The pass parameter contains the position of the text document in
		// which code complete got requested. For the example we ignore this
		// info and always provide the same completion items.

		return parseResponse.suggestions
			.map(sugestion => ({
				label: sugestion,
				kind: CompletionItemKind.Text,
				data: 1
			}));
	}
);

// This handler resolves additional information for the item selected in
// the completion list.
connection.onCompletionResolve(
	(item: CompletionItem): CompletionItem => {
		// if (item.data === 1) {
		// 	item.detail = 'TypeScript details';
		// 	item.documentation = 'TypeScript documentation';
		// } else if (item.data === 2) {
		// 	item.detail = 'JavaScript details';
		// 	item.documentation = 'JavaScript documentation';
		// }
		return item;
	}
);

connection.onDocumentFormatting(
	async (params: DocumentFormattingParams): Promise<TextEdit[]> => {
		// https://github.com/rubyide/vscode-ruby/blob/9f12c66741a937a5daf12699862dce8dde5ad4f6/packages/language-server-ruby/src/Formatter.ts#L60

		const { textDocument: ident } = params;
		const document = documents.get(ident.uri)!;

		const unformattedSql = document.getText();
		const parseResponse = await parseSql(unformattedSql);
		const formattedSql = parseResponse.formatted_sql;
		
		return processResults(document, unformattedSql, formattedSql);
	}
);

function processResults(document: TextDocument, originalText: string, output: string): TextEdit[] {
	const differ = new DiffMatchPatch();
	const diffs: Diff[] = differ.diff_main(originalText, output);
	const edits: TextEdit[] = [];
	// VSCode wants TextEdits on the original document
	// this means position only gets moved for DIFF_EQUAL and DIFF_DELETE
	// as insert is new and doesn't have a position in the original
	let position = 0;
	for (const diff of diffs) {
		const [num, str] = diff;
		const startPos = document.positionAt(position);

		switch (num) {
			case DIFF_DELETE:
				edits.push({
					range: {
						start: startPos,
						end: document.positionAt(position + str.length),
					},
					newText: '',
				});
				position += str.length;
				break;
			case DIFF_INSERT:
				edits.push({
					range: { start: startPos, end: startPos },
					newText: str,
				});
				break;
			case DIFF_EQUAL:
				position += str.length;
				break;
		}
	}

	return edits;
}

// Make the text document manager listen on the connection
// for open, change and close text document events
documents.listen(connection);

// Listen on the connection
connection.listen();
