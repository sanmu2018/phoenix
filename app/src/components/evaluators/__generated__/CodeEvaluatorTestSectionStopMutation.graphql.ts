/**
 * @generated SignedSource<<1ff9ea8c58b3c1bb5b898798cc97d909>>
 * @lightSyntaxTransform
 * @nogrep
 */

/* tslint:disable */
/* eslint-disable */
// @ts-nocheck

import { ConcreteRequest } from 'relay-runtime';
export type CodeEvaluatorTestSectionStopMutation$variables = {
  sessionId: string;
};
export type CodeEvaluatorTestSectionStopMutation$data = {
  readonly stopEvaluatorSession: {
    readonly sessionId: string;
    readonly stopped: boolean;
  };
};
export type CodeEvaluatorTestSectionStopMutation = {
  response: CodeEvaluatorTestSectionStopMutation$data;
  variables: CodeEvaluatorTestSectionStopMutation$variables;
};

const node: ConcreteRequest = (function(){
var v0 = [
  {
    "defaultValue": null,
    "kind": "LocalArgument",
    "name": "sessionId"
  }
],
v1 = [
  {
    "alias": null,
    "args": [
      {
        "kind": "Variable",
        "name": "sessionId",
        "variableName": "sessionId"
      }
    ],
    "concreteType": "StopEvaluatorSessionPayload",
    "kind": "LinkedField",
    "name": "stopEvaluatorSession",
    "plural": false,
    "selections": [
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "sessionId",
        "storageKey": null
      },
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "stopped",
        "storageKey": null
      }
    ],
    "storageKey": null
  }
];
return {
  "fragment": {
    "argumentDefinitions": (v0/*: any*/),
    "kind": "Fragment",
    "metadata": null,
    "name": "CodeEvaluatorTestSectionStopMutation",
    "selections": (v1/*: any*/),
    "type": "Mutation",
    "abstractKey": null
  },
  "kind": "Request",
  "operation": {
    "argumentDefinitions": (v0/*: any*/),
    "kind": "Operation",
    "name": "CodeEvaluatorTestSectionStopMutation",
    "selections": (v1/*: any*/)
  },
  "params": {
    "cacheID": "d753316287f4d34722e57488111715a3",
    "id": null,
    "metadata": {},
    "name": "CodeEvaluatorTestSectionStopMutation",
    "operationKind": "mutation",
    "text": "mutation CodeEvaluatorTestSectionStopMutation(\n  $sessionId: String!\n) {\n  stopEvaluatorSession(sessionId: $sessionId) {\n    sessionId\n    stopped\n  }\n}\n"
  }
};
})();

(node as any).hash = "20e09908708d0e77bb97f40da68433ef";

export default node;
