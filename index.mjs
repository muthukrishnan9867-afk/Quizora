
import {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  ScanCommand,
  BatchWriteItemCommand,
  QueryCommand
} from "@aws-sdk/client-dynamodb";

import {
  marshall,
  unmarshall
} from "@aws-sdk/util-dynamodb";

// ------------------------------------------------------------
// CONFIG
// ------------------------------------------------------------

const REGION =
  process.env.AWS_REGION ||
  process.env.AWS_DEFAULT_REGION ||
  "eu-north-1";

const TESTS_TABLE =
  process.env.TESTS_TABLE ||
  "QuizoraTests";

const RESULTS_TABLE =
  process.env.RESULTS_TABLE ||
  "QuizoraResults";

const STUDENTS_TABLE =
  process.env.STUDENTS_TABLE ||
  "QuizoraStudents";

const CREATORS_TABLE =
  process.env.CREATORS_TABLE ||
  "QuizoraCreators";

const ATTENDANCE_TABLE =
  process.env.ATTENDANCE_TABLE ||
  "MCQAttendance";

const ATTEMPTS_TABLE =
  process.env.ATTEMPTS_TABLE ||
  "MCQAttempts";

const QUESTIONS_TABLE =
  process.env.QUESTIONS_TABLE ||
  "QuizoraQuestions";

const CORS_ORIGIN =
  process.env.CORS_ORIGIN ||
  "*";

const dynamo = new DynamoDBClient({
  region: REGION
});

// ------------------------------------------------------------
// BASIC HELPERS
// ------------------------------------------------------------

function response(
  statusCode,
  body,
  extraHeaders = {}
) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": CORS_ORIGIN,
      "Access-Control-Allow-Headers":
        "Content-Type,Authorization,X-Requested-With",
      "Access-Control-Allow-Methods":
        "GET,POST,PUT,PATCH,DELETE,OPTIONS",
      "Cache-Control": "no-store",
      ...extraHeaders
    },
    body:
      typeof body === "string"
        ? body
        : JSON.stringify(body)
  };
}

function ok(body) {
  return response(200, body);
}

function created(body) {
  return response(201, body);
}

function badRequest(message, extra = {}) {
  return response(400, {
    success: false,
    message,
    ...extra
  });
}

function unauthorized(message = "Unauthorized") {
  return response(401, {
    success: false,
    message
  });
}

function forbidden(message = "Forbidden") {
  return response(403, {
    success: false,
    message
  });
}

function notFound(message = "Not found") {
  return response(404, {
    success: false,
    message
  });
}

function serverError(error) {
  console.error("SERVER ERROR:", error);
  console.error("Error stack:", error?.stack);

  return response(500, {
    success: false,
    message: "Internal server error",
    error: String(error?.message || error),
    stack: error?.stack
  });
}

function parseBody(event) {
  if (!event?.body) {
    return {};
  }

  if (typeof event.body === "object") {
    return event.body;
  }

  try {
    return JSON.parse(event.body);
  } catch {
    return {};
  }
}

function stringValue(value) {
  if (
    value === undefined ||
    value === null
  ) {
    return "";
  }

  return String(value).trim();
}

function normalizeEmail(value) {
  return stringValue(value).toLowerCase();
}

function makeId(prefix = "id") {
  return (
    prefix +
    "_" +
    Date.now().toString(36) +
    "_" +
    Math.random()
      .toString(36)
      .slice(2, 10)
  );
}

function nowIso() {
  return new Date().toISOString();
}

function toMillis(value) {
  const ms = Date.parse(value);

  return Number.isFinite(ms)
    ? ms
    : NaN;
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : fallback;
}

function roundNumber(value, decimals = 2) {
  const n = Number(value);

  if (!Number.isFinite(n)) {
    return 0;
  }

  const factor =
    Math.pow(10, decimals);

  return Math.round(n * factor) / factor;
}

// ------------------------------------------------------------
// PATH HELPERS
// ------------------------------------------------------------

function normalizePath(event) {
  let path =
    event?.rawPath ||
    event?.requestContext?.http?.path ||
    event?.path ||
    "/";

  path = String(path);

  // Remove API Gateway stage prefixes.
  path = path.replace(
    /^\/(?:prod|dev|test|stage)(?=\/|$)/i,
    ""
  );

  if (!path.startsWith("/")) {
    path = "/" + path;
  }

  // Remove trailing slash except root.
  if (
    path.length > 1 &&
    path.endsWith("/")
  ) {
    path = path.slice(0, -1);
  }

  return path;
}

function getMethod(event) {
  return String(
    event?.requestContext?.http?.method ||
    event?.httpMethod ||
    "GET"
  ).toUpperCase();
}

function getQuery(event) {
  return (
    event?.queryStringParameters ||
    {}
  );
}

function getPathParameters(event) {
  return (
    event?.pathParameters ||
    {}
  );
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (
      value !== undefined &&
      value !== null &&
      String(value).trim()
    ) {
      return value;
    }
  }

  return "";
}

function getStudentEmail(event, body = {}) {
  const query = getQuery(event);

  return normalizeEmail(
    firstNonEmpty(
      query.studentEmail,
      query.email,
      body.studentEmail,
      body.email,
      event?.headers?.["x-student-email"],
      event?.headers?.["X-Student-Email"]
    )
  );
}

// ------------------------------------------------------------
// QUESTION HELPERS
// ------------------------------------------------------------

function cleanAnswer(value) {
  const answer =
    stringValue(value).toUpperCase();

  if (
    answer === "A" ||
    answer === "B" ||
    answer === "C" ||
    answer === "D"
  ) {
    return answer;
  }

  return "";
}

function normalizeQuestion(question, index = 0) {
  const q =
    question &&
    typeof question === "object"
      ? question
      : {};

  const options = Array.isArray(q.options)
    ? q.options.slice(0, 4).map(
        item => stringValue(item)
      )
    : [
        stringValue(q.optionA),
        stringValue(q.optionB),
        stringValue(q.optionC),
        stringValue(q.optionD)
      ];

  while (options.length < 4) {
    options.push("");
  }

  return {
    id:
      stringValue(q.id) ||
      stringValue(q.questionId) ||
      `q_${index + 1}`,

    question:
      stringValue(
        q.question ??
        q.text ??
        q.questionText
      ),

    options,

    answer: cleanAnswer(
      q.answer ??
      q.correctAnswer
    ),

    marks: Math.max(
      0,
      safeNumber(
        q.marks,
        1
      )
    )
  };
}

function normalizeQuestions(questions) {
  if (!Array.isArray(questions)) {
    return [];
  }

  return questions.map(
    (q, index) =>
      normalizeQuestion(q, index)
  );
}

function validateQuestions(questions) {
  if (!questions.length) {
    return {
      valid: false,
      message:
        "At least one question is required."
    };
  }

  for (
    let i = 0;
    i < questions.length;
    i++
  ) {
    const q = questions[i];

    if (!q.question) {
      return {
        valid: false,
        message:
          `Question ${i + 1} is empty.`
      };
    }

    if (
      !Array.isArray(q.options) ||
      q.options.length !== 4
    ) {
      return {
        valid: false,
        message:
          `Question ${i + 1} must have exactly 4 options.`
      };
    }

    if (
      q.options.some(
        option =>
          !String(option || "").trim()
      )
    ) {
      return {
        valid: false,
        message:
          `Question ${i + 1} has an empty option.`
      };
    }

    if (
      !["A", "B", "C", "D"].includes(
        cleanAnswer(q.answer)
      )
    ) {
      return {
        valid: false,
        message:
          `Question ${i + 1} has an invalid correct answer.`
      };
    }

    if (
      ![
        1,
        2,
        3,
        4,
        5,
        10
      ].includes(
        Number(q.marks)
      )
    ) {
      return {
        valid: false,
        message:
          `Question ${i + 1} has invalid marks.`
      };
    }
  }

  return {
    valid: true
  };
}

function sanitizeQuestion(question) {
  return {
    id: question.id,
    question: question.question,
    options: Array.isArray(question.options)
      ? question.options
      : [],
    marks: safeNumber(
      question.marks,
      1
    )
  };
}

function sanitizeQuestions(questions) {
  return normalizeQuestions(
    questions
  ).map(
    sanitizeQuestion
  );
}

function calculateTotalMarks(questions) {
  return questions.reduce(
    (sum, q) =>
      sum +
      safeNumber(
        q.marks,
        1
      ),
    0
  );
}

// ------------------------------------------------------------
// TEST TIME HELPERS
// ------------------------------------------------------------

function hasStarted(test, currentMs = Date.now()) {
  const startMs =
    toMillis(test?.startTime);

  if (!Number.isFinite(startMs)) {
    return false;
  }

  return currentMs >= startMs;
}

function isExpired(test, currentMs = Date.now()) {
  const endMs =
    toMillis(test?.endTime);

  if (!Number.isFinite(endMs)) {
    return false;
  }

  return currentMs >= endMs;
}

function isCurrentlyLive(
  test,
  currentMs = Date.now()
) {
  if (
    test?.active === false ||
    test?.published === false
  ) {
    return false;
  }

  const startMs =
    toMillis(test?.startTime);

  const endMs =
    toMillis(test?.endTime);

  if (
    !Number.isFinite(startMs) ||
    !Number.isFinite(endMs)
  ) {
    return false;
  }

  return (
    currentMs >= startMs &&
    currentMs < endMs
  );
}

function getTestStatus(
  test,
  currentMs = Date.now()
) {
  if (test?.active === false) {
    return "INACTIVE";
  }

  if (test?.published === false) {
    return "DRAFT";
  }

  const startMs =
    toMillis(test?.startTime);

  const endMs =
    toMillis(test?.endTime);

  if (
    !Number.isFinite(startMs) ||
    !Number.isFinite(endMs)
  ) {
    return "SCHEDULE_ERROR";
  }

  if (currentMs < startMs) {
    return "UPCOMING";
  }

  if (
    currentMs >= startMs &&
    currentMs < endMs
  ) {
    return "LIVE";
  }

  return "EXPIRED";
}

function calculateLiveHours(
  startTime,
  endTime
) {
  const startMs =
    toMillis(startTime);

  const endMs =
    toMillis(endTime);

  if (
    !Number.isFinite(startMs) ||
    !Number.isFinite(endMs) ||
    endMs <= startMs
  ) {
    return 0;
  }

  return roundNumber(
    (endMs - startMs) /
      (1000 * 60 * 60),
    4
  );
}

function calculateEndTime(
  startTime,
  liveHours
) {
  const startMs =
    toMillis(startTime);

  const hours =
    safeNumber(
      liveHours,
      0
    );

  if (
    !Number.isFinite(startMs) ||
    hours <= 0
  ) {
    return "";
  }

  return new Date(
    startMs +
      hours *
        60 *
        60 *
        1000
  ).toISOString();
}

function calculateAttemptDeadline(
  test,
  startedAt
) {
  const startedMs =
    toMillis(startedAt);

  const durationMinutes =
    safeNumber(
      test?.durationMinutes,
      0
    );

  const testEndMs =
    toMillis(test?.endTime);

  if (
    !Number.isFinite(startedMs) ||
    durationMinutes <= 0
  ) {
    return test?.endTime || "";
  }

  const durationEndMs =
    startedMs +
    durationMinutes *
      60 *
      1000;

  const finalMs =
    Number.isFinite(testEndMs)
      ? Math.min(
          durationEndMs,
          testEndMs
        )
      : durationEndMs;

  return new Date(
    finalMs
  ).toISOString();
}

// ------------------------------------------------------------
// ASSIGNMENT HELPERS
// ------------------------------------------------------------

function getAssignedEmails(test) {
  if (
    !Array.isArray(
      test?.assignedEmails
    )
  ) {
    return [];
  }

  return [
    ...new Set(
      test.assignedEmails
        .map(normalizeEmail)
        .filter(Boolean)
    )
  ];
}

function isAssignedToStudent(
  test,
  email
) {
  const normalized =
    normalizeEmail(email);

  if (!normalized) {
    return false;
  }

  const assigned =
    getAssignedEmails(test);

  // Empty assignment = ALL STUDENTS.
  if (!assigned.length) {
    return true;
  }

  return assigned.includes(
    normalized
  );
}

// ------------------------------------------------------------
// TEST RESPONSE OBJECT
// ------------------------------------------------------------

function creatorTest(test) {
  const questions =
    normalizeQuestions(
      test.questions
    );

  return {
    testId: test.testId,
    testCode: test.testCode,
    title: test.title,

    questions,

    totalQuestions:
      questions.length,

    totalMarks:
      calculateTotalMarks(
        questions
      ),

    liveHours:
      safeNumber(
        test.liveHours,
        calculateLiveHours(
          test.startTime,
          test.endTime
        )
      ),

    durationMinutes:
      safeNumber(
        test.durationMinutes,
        0
      ),

    startTime:
      test.startTime || "",

    endTime:
      test.endTime || "",

    assignedEmails:
      getAssignedEmails(test),

    assignmentType:
      stringValue(
        test.assignmentType
      ) ||
      (
        getAssignedEmails(test).length
          ? "email"
          : "code"
      ),

    requiredDetails:
      test.requiredDetails ||
      {
        name: true,
        email: true,
        school: false,
        regNo: false,
        custom: []
      },

    active:
      test.active !== false,

    published:
      test.published === true,

    status:
      getTestStatus(test),

    createdAt:
      test.createdAt || "",

    updatedAt:
      test.updatedAt || ""
  };
}

function studentTest(
  test,
  currentMs = Date.now()
) {
  const questions =
    normalizeQuestions(
      test.questions
    );

  const endMs =
    toMillis(test.endTime);

  let remainingSeconds = 0;

  if (Number.isFinite(endMs)) {
    remainingSeconds =
      Math.max(
        0,
        Math.floor(
          (endMs - currentMs) /
            1000
        )
      );
  }

  return {
    testId: test.testId,
    testCode: test.testCode,
    title: test.title,

    questions:
      sanitizeQuestions(
        questions
      ),

    totalQuestions:
      questions.length,

    totalMarks:
      calculateTotalMarks(
        questions
      ),

    durationMinutes:
      safeNumber(
        test.durationMinutes,
        0
      ),

    liveHours:
      safeNumber(
        test.liveHours,
        calculateLiveHours(
          test.startTime,
          test.endTime
        )
      ),

    startTime:
      test.startTime || "",

    endTime:
      test.endTime || "",

    remainingSeconds,

    status:
      getTestStatus(
        test,
        currentMs
      ),

    active:
      test.active !== false,

    published:
      test.published === true,

    requiredDetails:
      test.requiredDetails ||
      {
        name: true,
        email: true,
        school: false,
        regNo: false,
        custom: []
      }
  };
}

// ------------------------------------------------------------
// DYNAMODB TEST HELPERS
// ------------------------------------------------------------

async function getTest(testId) {
  const result =
    await dynamo.send(
      new GetItemCommand({
        TableName:
          TESTS_TABLE,
        Key: marshall({
          testId
        })
      })
    );

  if (!result.Item) {
    return null;
  }

  const test = unmarshall(
    result.Item
  );

  // Fetch questions from separate table
  const separateQuestions = await getQuestions(testId);

  // Use separate questions if available, fallback to nested questions
  if (separateQuestions.length > 0) {
    test.questions = separateQuestions;
  } else if (!test.questions || !Array.isArray(test.questions)) {
    test.questions = [];
  }

  return test;
}

async function saveTest(test) {
  await dynamo.send(
    new PutItemCommand({
      TableName:
        TESTS_TABLE,
      Item: marshall(
        test,
        {
          removeUndefinedValues:
            true
        }
      )
    })
  );

  return test;
}

async function deleteTestById(testId) {
  // Delete test and all its questions
  await Promise.all([
    dynamo.send(
      new DeleteItemCommand({
        TableName:
          TESTS_TABLE,
        Key: marshall({
          testId
        })
      })
    ),
    deleteQuestions(testId)
  ]);
}

async function getAllTests() {
  const items = [];
  let ExclusiveStartKey;

  do {
    const result =
      await dynamo.send(
        new ScanCommand({
          TableName:
            TESTS_TABLE,
          ExclusiveStartKey
        })
      );

    if (Array.isArray(result.Items)) {
      for (const item of result.Items) {
        items.push(
          unmarshall(item)
        );
      }
    }

    ExclusiveStartKey =
      result.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return items;
}

// ------------------------------------------------------------
// DYNAMODB QUESTIONS HELPERS
// ------------------------------------------------------------

async function saveQuestions(testId, questions) {
  if (!testId || !Array.isArray(questions) || !questions.length) {
    return;
  }

  // DynamoDB batch write limit: 25 items
  const BATCH_SIZE = 25;

  for (let i = 0; i < questions.length; i += BATCH_SIZE) {
    const batch = questions.slice(i, i + BATCH_SIZE);

    const writeRequests = batch.map(question => ({
      PutRequest: {
        Item: marshall({
          testId,
          questionId: question.id,
          question: question.question,
          options: question.options,
          answer: question.answer,
          marks: question.marks,
          orderIndex: questions.indexOf(question)
        }, { removeUndefinedValues: true })
      }
    }));

    await dynamo.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [QUESTIONS_TABLE]: writeRequests
        }
      })
    );
  }
}

async function getQuestions(testId) {
  if (!testId) {
    return [];
  }

  const result = await dynamo.send(
    new QueryCommand({
      TableName: QUESTIONS_TABLE,
      KeyConditionExpression: "testId = :testId",
      ExpressionAttributeValues: marshall({
        ":testId": testId
      })
    })
  );

  if (!Array.isArray(result.Items)) {
    return [];
  }

  const questions = result.Items.map(item => unmarshall(item));

  // Sort by orderIndex to maintain question order
  questions.sort((a, b) =>
    (a.orderIndex || 0) - (b.orderIndex || 0)
  );

  return questions.map(q => ({
    id: q.questionId,
    question: q.question,
    options: q.options,
    answer: q.answer,
    marks: q.marks
  }));
}

async function deleteQuestions(testId) {
  if (!testId) {
    return;
  }

  const questions = await getQuestions(testId);

  if (!questions.length) {
    return;
  }

  const BATCH_SIZE = 25;

  for (let i = 0; i < questions.length; i += BATCH_SIZE) {
    const batch = questions.slice(i, i + BATCH_SIZE);

    const deleteRequests = batch.map(question => ({
      DeleteRequest: {
        Key: marshall({
          testId,
          questionId: question.id
        })
      }
    }));

    await dynamo.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [QUESTIONS_TABLE]: deleteRequests
        }
      })
    );
  }
}

// ------------------------------------------------------------
// DYNAMODB STUDENT HELPERS
// ------------------------------------------------------------

async function getStudent(
  studentEmail
) {
  const email =
    normalizeEmail(
      studentEmail
    );

  if (!email) {
    return null;
  }

  const result =
    await dynamo.send(
      new GetItemCommand({
        TableName:
          STUDENTS_TABLE,
        Key: marshall({
          studentId: email
        })
      })
    );

  if (!result.Item) {
    return null;
  }

  return unmarshall(
    result.Item
  );
}

async function getAllStudents() {
  const students = [];
  let ExclusiveStartKey;

  do {
    const result =
      await dynamo.send(
        new ScanCommand({
          TableName:
            STUDENTS_TABLE,
          ExclusiveStartKey
        })
      );

    if (Array.isArray(result.Items)) {
      for (const item of result.Items) {
        const student =
          unmarshall(item);

        const email =
          normalizeEmail(
            student.studentEmail ||
            student.email
          );

        if (email) {
          students.push({
            ...student,
            studentEmail: email
          });
        }
      }
    }

    ExclusiveStartKey =
      result.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return students;
}

async function findStudentByEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return null;

  try {
    const result = await dynamo.send(
      new ScanCommand({
        TableName: STUDENTS_TABLE,
        FilterExpression: "email = :email",
        ExpressionAttributeValues: marshall({
          ":email": normalized
        })
      })
    );

    if (result.Items && result.Items.length > 0) {
      return unmarshall(result.Items[0]);
    }
    return null;
  } catch (error) {
    console.error("findStudentByEmail error:", error);
    return null;
  }
}

// ------------------------------------------------------------
// CREATOR HELPERS
// ------------------------------------------------------------

async function getAllCreators() {
  const creators = [];
  let ExclusiveStartKey;

  do {
    const result =
      await dynamo.send(
        new ScanCommand({
          TableName:
            CREATORS_TABLE,
          ExclusiveStartKey
        })
      );

    if (Array.isArray(result.Items)) {
      for (const item of result.Items) {
        const creator =
          unmarshall(item);

        const email =
          normalizeEmail(
            creator.creatorEmail ||
            creator.email
          );

        if (email) {
          creators.push({
            ...creator,
            creatorEmail: email
          });
        }
      }
    }

    ExclusiveStartKey =
      result.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return creators;
}

async function findCreatorByEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return null;

  try {
    const result = await dynamo.send(
      new ScanCommand({
        TableName: CREATORS_TABLE,
        FilterExpression: "email = :email",
        ExpressionAttributeValues: marshall({
          ":email": normalized
        })
      })
    );

    if (result.Items && result.Items.length > 0) {
      return unmarshall(result.Items[0]);
    }
    return null;
  } catch (error) {
    console.error("findCreatorByEmail error:", error);
    return null;
  }
}

// ------------------------------------------------------------
// RESULT HELPERS
// ------------------------------------------------------------

function makeResultId(
  testId,
  studentEmail
) {
  return (
    `${testId}__${normalizeEmail(
      studentEmail
    )}`
  );
}

async function getResult(
  testId,
  studentEmail
) {
  const email =
    normalizeEmail(
      studentEmail
    );

  if (!testId || !email) {
    return null;
  }

  const result =
    await dynamo.send(
      new GetItemCommand({
        TableName:
          RESULTS_TABLE,
        Key: marshall({
          resultId:
            makeResultId(
              testId,
              email
            )
        })
      })
    );

  if (!result.Item) {
    return null;
  }

  return unmarshall(
    result.Item
  );
}

async function saveResult(result) {
  await dynamo.send(
    new PutItemCommand({
      TableName:
        RESULTS_TABLE,
      Item: marshall(
        result,
        {
          removeUndefinedValues:
            true
        }
      )
    })
  );

  return result;
}

async function getAllResults() {
  const results = [];
  let ExclusiveStartKey;

  do {
    const result =
      await dynamo.send(
        new ScanCommand({
          TableName:
            RESULTS_TABLE,
          ExclusiveStartKey
        })
      );

    if (Array.isArray(result.Items)) {
      for (const item of result.Items) {
        results.push(
          unmarshall(item)
        );
      }
    }

    ExclusiveStartKey =
      result.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return results;
}

function extractStudentDetails(source = {}) {
  const details = {};

  [
    "name",
    "email",
    "school",
    "regNo",
    "rollNumber",
    "registerNumber",
    "collegeName"
  ].forEach(field => {
    const value = stringValue(source?.[field]);
    if (value) details[field] = value;
  });

  if (source && typeof source === "object") {
    Object.keys(source)
      .filter(key => key.startsWith("custom_"))
      .forEach(key => {
        const value = stringValue(source[key]);
        if (value) details[key] = value;
      });
  }

  return details;
}

function sanitizeResult(result) {
  if (!result) {
    return null;
  }

  return {
    resultId:
      result.resultId,

    testId:
      result.testId,

    testCode:
      result.testCode,

    studentEmail:
      normalizeEmail(
        result.studentEmail
      ),

    studentName:
      result.studentName ||
      "",

    ...extractStudentDetails(result),

    testTitle:
      result.testTitle ||
      "",

    score:
      safeNumber(
        result.score,
        0
      ),

    totalMarks:
      safeNumber(
        result.totalMarks,
        0
      ),

    percentage:
      roundNumber(
        safeNumber(
          result.percentage,
          0
        ),
        2
      ),

    correct:
      safeNumber(
        result.correct,
        0
      ),

    wrong:
      safeNumber(
        result.wrong,
        0
      ),

    unanswered:
      safeNumber(
        result.unanswered,
        0
      ),

    totalQuestions:
      safeNumber(
        result.totalQuestions,
        0
      ),

    submittedAt:
      result.submittedAt ||
      "",

    startedAt:
      result.startedAt ||
      "",

    attemptEndTime:
      result.attemptEndTime ||
      "",

    status:
      result.status ||
      "COMPLETED"
  };
}

// ------------------------------------------------------------
// ANSWER NORMALIZATION
// ------------------------------------------------------------

function normalizeAnswers(answers) {
  if (!answers) {
    return {};
  }

  if (
    Array.isArray(answers)
  ) {
    const output = {};

    for (
      let i = 0;
      i < answers.length;
      i++
    ) {
      const item =
        answers[i];

      if (
        item &&
        typeof item === "object"
      ) {
        const id =
          stringValue(
            item.questionId ||
            item.id ||
            item.question
          );

        if (id) {
          output[id] =
            cleanAnswer(
              item.answer ||
              item.selectedAnswer
            );
        }
      }
    }

    return output;
  }

  if (
    typeof answers === "object"
  ) {
    const output = {};

    for (
      const [key, value]
      of Object.entries(answers)
    ) {
      if (
        value &&
        typeof value === "object"
      ) {
        output[key] =
          cleanAnswer(
            value.answer ||
            value.selectedAnswer
          );
      } else {
        output[key] =
          cleanAnswer(value);
      }
    }

    return output;
  }

  return {};
}

// ------------------------------------------------------------
// ATTENDANCE HELPERS
// ------------------------------------------------------------

function makeAttendanceId(
  testId,
  studentEmail
) {
  return makeResultId(
    testId,
    studentEmail
  );
}

async function getAttendance(
  testId,
  studentEmail
) {
  const email =
    normalizeEmail(
      studentEmail
    );

  if (!testId || !email) {
    return null;
  }

  const result =
    await dynamo.send(
      new GetItemCommand({
        TableName:
          ATTENDANCE_TABLE,
        Key: marshall({
          attendanceId:
            makeAttendanceId(
              testId,
              email
            )
        })
      })
    );

  if (!result.Item) {
    return null;
  }

  return unmarshall(
    result.Item
  );
}

async function saveAttendance(
  attendance
) {
  await dynamo.send(
    new PutItemCommand({
      TableName:
        ATTENDANCE_TABLE,
      Item: marshall(
        attendance,
        {
          removeUndefinedValues:
            true
        }
      )
    })
  );

  return attendance;
}

async function getAllAttendance() {
  const attendance = [];
  let ExclusiveStartKey;

  do {
    const result =
      await dynamo.send(
        new ScanCommand({
          TableName:
            ATTENDANCE_TABLE,
          ExclusiveStartKey
        })
      );

    if (Array.isArray(result.Items)) {
      for (const item of result.Items) {
        attendance.push(
          unmarshall(item)
        );
      }
    }

    ExclusiveStartKey =
      result.LastEvaluatedKey;
  } while (ExclusiveStartKey);

  return attendance;
}

async function startAttendance(
  test,
  studentEmail,
  studentName = "",
  studentDetails = {}
) {
  const email =
    normalizeEmail(
      studentEmail
    );

  const existing =
    await getAttendance(
      test.testId,
      email
    );

  if (existing) {
    return {
      ...existing,
      alreadyStarted: true
    };
  }

  const startedAt =
    nowIso();

  const attemptEndTime =
    calculateAttemptDeadline(
      test,
      startedAt
    );

  const attendance = {
    attendanceId:
      makeAttendanceId(
        test.testId,
        email
      ),

    testId:
      test.testId,

    testCode:
      test.testCode,

    testTitle:
      test.title,

    studentEmail:
      email,

    studentName:
      studentName || "",

    ...(
      studentDetails &&
      typeof studentDetails === "object"
        ? studentDetails
        : {}
    ),

    startedAt,

    attemptEndTime,

    status:
      "IN_PROGRESS",

    lastHeartbeatAt:
      startedAt,

    updatedAt:
      startedAt,

    alreadyStarted: false
  };

  return saveAttendance(
    attendance
  );
}

async function heartbeatAttendance(
  attendance
) {
  const updatedAt =
    nowIso();

  await dynamo.send(
    new UpdateItemCommand({
      TableName:
        ATTENDANCE_TABLE,

      Key: marshall({
        attendanceId:
          attendance.attendanceId
      }),

      UpdateExpression:
        "SET lastHeartbeatAt = :heartbeat, updatedAt = :updatedAt",

      ExpressionAttributeValues:
        marshall({
          ":heartbeat":
            updatedAt,

          ":updatedAt":
            updatedAt
        })
    })
  );

  return {
    ...attendance,
    lastHeartbeatAt:
      updatedAt,
    updatedAt
  };
}

// ------------------------------------------------------------
// ATTEMPT HELPERS
// ------------------------------------------------------------

function makeAttemptId(
  testId,
  studentEmail
) {
  return makeResultId(
    testId,
    studentEmail
  );
}

async function getAttempt(
  testId,
  studentEmail
) {
  const email =
    normalizeEmail(
      studentEmail
    );

  if (!testId || !email) {
    return null;
  }

  const result =
    await dynamo.send(
      new GetItemCommand({
        TableName:
          ATTEMPTS_TABLE,
        Key: marshall({
          attemptId:
            makeAttemptId(
              testId,
              email
            )
        })
      })
    );

  if (!result.Item) {
    return null;
  }

  return unmarshall(
    result.Item
  );
}

async function saveAttempt(
  attempt
) {
  await dynamo.send(
    new PutItemCommand({
      TableName:
        ATTEMPTS_TABLE,
      Item: marshall(
        attempt,
        {
          removeUndefinedValues:
            true
        }
      )
    })
  );

  return attempt;
}

async function getOrCreateAttempt(
  testId,
  studentEmail
) {
  try {
    const email =
      normalizeEmail(
        studentEmail
      );

    if (!testId || !email) {
      return null;
    }

    let attempt =
      await getAttempt(
        testId,
        email
      );

    if (!attempt) {
      attempt = {
        attemptId:
          makeAttemptId(
            testId,
            email
          ),
        testId,
        studentEmail:
          email,
        status:
          "ACTIVE",
        violationCount: 0,
        violations: [],
        createdAt:
          nowIso(),
        updatedAt:
          nowIso()
      };

      await saveAttempt(
        attempt
      );
    } else if (
      attempt.status ===
      "RESUMED"
    ) {
      attempt.status =
        "ACTIVE";
      attempt.violationCount = 0;
      attempt.violations = [];
      attempt.updatedAt =
        nowIso();

      await saveAttempt(
        attempt
      );
    }

    return attempt;
  } catch (error) {
    console.error(
      "getOrCreateAttempt error:",
      error
    );
    console.error(
      "Make sure MCQAttempts table exists in DynamoDB"
    );

    return null;
  }
}

// ------------------------------------------------------------
// CREATE TEST
// ------------------------------------------------------------

async function createTest(body) {
  const title =
    stringValue(
      body.title
    );

  if (!title) {
    return badRequest(
      "Test title is required."
    );
  }

  const questions =
    normalizeQuestions(
      body.questions
    );

  const validation =
    validateQuestions(
      questions
    );

  if (!validation.valid) {
    return badRequest(
      validation.message
    );
  }

  const liveHours =
    safeNumber(
      body.liveHours,
      0
    );

  const durationMinutes =
    safeNumber(
      body.durationMinutes,
      0
    );

  if (liveHours <= 0) {
    return badRequest(
      "liveHours must be greater than 0."
    );
  }

  if (durationMinutes <= 0) {
    return badRequest(
      "durationMinutes must be greater than 0."
    );
  }

  const startTime =
    stringValue(
      body.startTime
    ) ||
    nowIso();

  if (
    !Number.isFinite(
      toMillis(startTime)
    )
  ) {
    return badRequest(
      "Invalid startTime."
    );
  }

  const endTime =
    calculateEndTime(
      startTime,
      liveHours
    );

  if (!endTime) {
    return badRequest(
      "Unable to calculate endTime."
    );
  }

  const assignedEmails =
    Array.isArray(
      body.assignedEmails
    )
      ? [
          ...new Set(
            body.assignedEmails
              .map(
                normalizeEmail
              )
              .filter(Boolean)
          )
        ]
      : [];

  const createdAt =
    nowIso();

  const testId =
    stringValue(
      body.testId
    ) ||
    makeId("test");

  const testCode =
    stringValue(
      body.testCode
    ) ||
    Math.random()
      .toString(36)
      .slice(2, 8)
      .toUpperCase();

  const assignmentType =
    stringValue(
      body.assignmentType
    ) ||
    (
      assignedEmails.length > 0
        ? "email"
        : "code"
    );

  const test = {
    testId,
    testCode,

    title,

    // Don't store questions in test object anymore
    // questions field removed

    totalQuestions:
      questions.length,

    totalMarks:
      calculateTotalMarks(
        questions
      ),

    liveHours,

    durationMinutes,

    startTime,

    endTime,

    assignmentType,

    assignedEmails,

    requiredDetails:
      body.requiredDetails ||
      {
        name: true,
        email: true,
        school: false,
        regNo: false,
        custom: []
      },

    active:
      body.active !== false,

    published:
      body.published === true,

    createdAt,

    updatedAt:
      createdAt
  };

  // Save test and questions separately
  await Promise.all([
    saveTest(test),
    saveQuestions(testId, questions)
  ]);

  // Attach questions for response
  test.questions = questions;

  return created({
    success: true,
    message:
      "Test created successfully.",
    test:
      creatorTest(test)
  });
}

// ------------------------------------------------------------
// UPDATE TEST
// ------------------------------------------------------------

async function updateTest(
  testId,
  body
) {
  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const existing =
    await getTest(testId);

  if (!existing) {
    return notFound(
      "Test not found."
    );
  }

  const updated = {
    ...existing
  };

  if (
    body.title !== undefined
  ) {
    const title =
      stringValue(
        body.title
      );

    if (!title) {
      return badRequest(
        "Test title cannot be empty."
      );
    }

    updated.title =
      title;
  }

  let questionsUpdated = false;
  let newQuestions = null;

  if (
    body.questions !== undefined
  ) {
    const questions =
      normalizeQuestions(
        body.questions
      );

    const validation =
      validateQuestions(
        questions
      );

    if (!validation.valid) {
      return badRequest(
        validation.message
      );
    }

    // Don't store in test object
    // Will save to separate table after

    questionsUpdated = true;
    newQuestions = questions;

    updated.totalQuestions =
      questions.length;

    updated.totalMarks =
      calculateTotalMarks(
        questions
      );
  }

  if (
    body.durationMinutes !==
    undefined
  ) {
    const duration =
      safeNumber(
        body.durationMinutes,
        0
      );

    if (duration <= 0) {
      return badRequest(
        "durationMinutes must be greater than 0."
      );
    }

    updated.durationMinutes =
      duration;
  }

  if (
    body.assignedEmails !==
    undefined
  ) {
    updated.assignedEmails =
      Array.isArray(
        body.assignedEmails
      )
        ? [
            ...new Set(
              body.assignedEmails
                .map(
                  normalizeEmail
                )
                .filter(Boolean)
            )
          ]
        : [];
  }

  if (
    body.assignmentType !==
    undefined
  ) {
    updated.assignmentType =
      stringValue(
        body.assignmentType
      ) ||
      "code";
  }

  if (
    body.active !== undefined
  ) {
    updated.active =
      Boolean(
        body.active
      );
  }

  if (
    body.published !== undefined
  ) {
    updated.published =
      Boolean(
        body.published
      );
  }

  // ----------------------------------------------------------
  // TIME UPDATE RULES
  //
  // 1. startTime + liveHours:
  //      end = start + liveHours
  //
  // 2. only startTime:
  //      keep existing liveHours
  //
  // 3. only liveHours:
  //      keep existing startTime
  //
  // 4. only endTime:
  //      calculate liveHours
  //
  // 5. startTime + endTime:
  //      calculate liveHours
  //
  // 6. startTime + liveHours + endTime:
  //      start + liveHours is source of truth
  // ----------------------------------------------------------

  const hasStart =
    body.startTime !==
    undefined;

  const hasEnd =
    body.endTime !==
    undefined;

  const hasLiveHours =
    body.liveHours !==
    undefined;

  if (
    hasStart &&
    hasLiveHours
  ) {
    const start =
      stringValue(
        body.startTime
      );

    const hours =
      safeNumber(
        body.liveHours,
        0
      );

    if (
      !Number.isFinite(
        toMillis(start)
      )
    ) {
      return badRequest(
        "Invalid startTime."
      );
    }

    if (hours <= 0) {
      return badRequest(
        "liveHours must be greater than 0."
      );
    }

    updated.startTime =
      start;

    updated.liveHours =
      hours;

    updated.endTime =
      calculateEndTime(
        start,
        hours
      );
  } else if (
    hasStart &&
    !hasLiveHours &&
    !hasEnd
  ) {
    const start =
      stringValue(
        body.startTime
      );

    if (
      !Number.isFinite(
        toMillis(start)
      )
    ) {
      return badRequest(
        "Invalid startTime."
      );
    }

    updated.startTime =
      start;

    updated.endTime =
      calculateEndTime(
        start,
        safeNumber(
          existing.liveHours,
          0
        )
      );

    updated.liveHours =
      calculateLiveHours(
        updated.startTime,
        updated.endTime
      );
  } else if (
    hasLiveHours &&
    !hasStart &&
    !hasEnd
  ) {
    const hours =
      safeNumber(
        body.liveHours,
        0
      );

    if (hours <= 0) {
      return badRequest(
        "liveHours must be greater than 0."
      );
    }

    updated.liveHours =
      hours;

    updated.endTime =
      calculateEndTime(
        updated.startTime,
        hours
      );
  } else if (
    hasEnd &&
    !hasStart &&
    !hasLiveHours
  ) {
    const end =
      stringValue(
        body.endTime
      );

    if (
      !Number.isFinite(
        toMillis(end)
      )
    ) {
      return badRequest(
        "Invalid endTime."
      );
    }

    const start =
      updated.startTime;

    if (
      toMillis(end) <=
      toMillis(start)
    ) {
      return badRequest(
        "endTime must be after startTime."
      );
    }

    updated.endTime =
      end;

    updated.liveHours =
      calculateLiveHours(
        start,
        end
      );
  } else if (
    hasStart &&
    hasEnd &&
    !hasLiveHours
  ) {
    const start =
      stringValue(
        body.startTime
      );

    const end =
      stringValue(
        body.endTime
      );

    if (
      !Number.isFinite(
        toMillis(start)
      ) ||
      !Number.isFinite(
        toMillis(end)
      )
    ) {
      return badRequest(
        "Invalid startTime or endTime."
      );
    }

    if (
      toMillis(end) <=
      toMillis(start)
    ) {
      return badRequest(
        "endTime must be after startTime."
      );
    }

    updated.startTime =
      start;

    updated.endTime =
      end;

    updated.liveHours =
      calculateLiveHours(
        start,
        end
      );
  }

  if (
    !updated.startTime ||
    !updated.endTime
  ) {
    return badRequest(
      "Test must have valid startTime and endTime."
    );
  }

  if (
    toMillis(
      updated.endTime
    ) <=
    toMillis(
      updated.startTime
    )
  ) {
    return badRequest(
      "endTime must be after startTime."
    );
  }

  if (
    body.requiredDetails !==
    undefined
  ) {
    updated.requiredDetails =
      body.requiredDetails ||
      {
        name: true,
        email: true,
        school: false,
        regNo: false,
        custom: []
      };
  }

  updated.updatedAt =
    nowIso();

  // Save test and update questions if changed
  if (questionsUpdated && newQuestions) {
    await Promise.all([
      saveTest(updated),
      deleteQuestions(testId),
      saveQuestions(testId, newQuestions)
    ]);

    // Attach questions for response
    updated.questions = newQuestions;
  } else {
    await saveTest(updated);

    // Fetch existing questions for response
    const existingQuestions = await getQuestions(testId);
    if (existingQuestions.length > 0) {
      updated.questions = existingQuestions;
    }
  }

  return ok({
    success: true,
    message:
      "Test updated successfully.",
    test:
      creatorTest(updated)
  });
}

// ------------------------------------------------------------
// GET CREATOR TESTS
// ------------------------------------------------------------

async function listCreatorTests() {
  const tests =
    await getAllTests();

  tests.sort(
    (a, b) =>
      String(
        b.createdAt || ""
      ).localeCompare(
        String(
          a.createdAt || ""
        )
      )
  );

  return ok({
    success: true,
    tests:
      tests.map(
        creatorTest
      )
  });
}

// ------------------------------------------------------------
// STUDENT TEST LIST
//
// IMPORTANT:
//
// A completed test is included even after its live window
// expires. This allows the student portal to show:
//
//       ✓ Completed
//
// instead of making the test disappear immediately.
//
// Uncompleted expired tests are still hidden.
// ------------------------------------------------------------

async function listStudentTests(
  event
) {
  const email =
    getStudentEmail(
      event
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  const currentMs =
    Date.now();

  const [
    tests,
    allResults
  ] = await Promise.all([
    getAllTests(),
    getAllResults()
  ]);

  const studentResults =
    new Map();

  for (
    const result
    of allResults
  ) {
    if (
      normalizeEmail(
        result.studentEmail
      ) === email
    ) {
      studentResults.set(
        result.testId,
        result
      );
    }
  }

  const visibleTests = [];

  for (
    const test
    of tests
  ) {
    if (
      test.active === false ||
      test.published !== true
    ) {
      continue;
    }

    if (
      !isAssignedToStudent(
        test,
        email
      )
    ) {
      continue;
    }

    const result =
      studentResults.get(
        test.testId
      );

    const live =
      isCurrentlyLive(
        test,
        currentMs
      );

    // Show:
    // 1. currently live tests
    // 2. completed tests
    //
    // Hide expired + not completed.
    if (
      !live &&
      !result
    ) {
      continue;
    }

    const card =
      studentTest(
        test,
        currentMs
      );

    if (result) {
      card.completed =
        true;

      card.completionStatus =
        "COMPLETED";

      card.canStart =
        false;

      card.result =
        sanitizeResult(
          result
        );
    } else {
      card.completed =
        false;

      card.completionStatus =
        "AVAILABLE";

      card.canStart =
        true;

      card.result =
        null;
    }

    visibleTests.push(
      card
    );
  }

  // Live tests first.
  // Completed tests afterwards.
  visibleTests.sort(
    (a, b) => {
      if (
        a.completionStatus !==
        b.completionStatus
      ) {
        return (
          a.completionStatus ===
          "AVAILABLE"
            ? -1
            : 1
        );
      }

      return String(
        a.endTime || ""
      ).localeCompare(
        String(
          b.endTime || ""
        )
      );
    }
  );

  return ok({
    success: true,
    studentEmail:
      email,
    tests:
      visibleTests,
    total:
      visibleTests.length
  });
}

// ------------------------------------------------------------
// GET SINGLE STUDENT TEST
// ------------------------------------------------------------

async function getStudentTest(
  event,
  testId
) {
  const email =
    getStudentEmail(
      event
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const test =
    await getTest(testId);

  if (!test) {
    return notFound(
      "Test not found."
    );
  }

  if (
    !isAssignedToStudent(
      test,
      email
    )
  ) {
    return forbidden(
      "This test is not assigned to this student."
    );
  }

  if (
    !isCurrentlyLive(test)
  ) {
    return forbidden(
      "This test is not currently live."
    );
  }

  const result =
    await getResult(
      testId,
      email
    );

  if (result) {
    return ok({
      success: true,
      completed: true,
      completionStatus:
        "COMPLETED",
      test: studentTest(
        test
      ),
      result:
        sanitizeResult(
          result
        )
    });
  }

  return ok({
    success: true,
    completed: false,
    completionStatus:
      "AVAILABLE",
    test:
      studentTest(test),
    result: null
  });
}

// ------------------------------------------------------------
// TEST BY CODE
// ------------------------------------------------------------

async function getTestByCode(
  event,
  code
) {
  const email =
    getStudentEmail(
      event
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  const testCode =
    stringValue(
      code
    ).toUpperCase();

  if (!testCode) {
    return badRequest(
      "Test code is required."
    );
  }

  const tests =
    await getAllTests();

  const test =
    tests.find(
      item =>
        stringValue(
          item.testCode
        ).toUpperCase() ===
        testCode
    );

  if (!test) {
    return notFound(
      "Test not found."
    );
  }

  if (
    !isAssignedToStudent(
      test,
      email
    )
  ) {
    return forbidden(
      "This test is not assigned to this student."
    );
  }

  const result =
    await getResult(
      test.testId,
      email
    );

  if (result) {
    return ok({
      success: true,
      completed: true,
      completionStatus:
        "COMPLETED",
      test:
        studentTest(test),
      result:
        sanitizeResult(
          result
        )
    });
  }

  if (
    !isCurrentlyLive(test)
  ) {
    return forbidden(
      "This test is not currently live."
    );
  }

  return ok({
    success: true,
    completed: false,
    completionStatus:
      "AVAILABLE",
    test:
      studentTest(test),
    result: null
  });
}

// ------------------------------------------------------------
// START ATTENDANCE
// ------------------------------------------------------------

async function handleStartAttendance(
  event
) {
  try {
    console.log(
      "handleStartAttendance called"
    );

    const body =
      parseBody(event);

    console.log(
      "Request body keys:",
      Object.keys(body)
    );

    const email =
      getStudentEmail(
        event,
        body
      );

    console.log(
      "Student email:",
      email
    );

    const testId =
      stringValue(
        body.testId
      );

    console.log(
      "Test ID:",
      testId
    );

    const studentName =
      stringValue(
        body.studentName ||
        body.name
      );

    console.log(
      "Student name:",
      studentName
    );

    if (!email) {
      return badRequest(
        "studentEmail is required."
      );
    }

    if (!testId) {
      return badRequest(
        "testId is required."
      );
    }

    console.log(
      "Fetching test..."
    );

    const test =
      await getTest(testId);

    if (!test) {
      return notFound(
        "Test not found."
      );
    }

    console.log(
      "Test found, checking assignment..."
    );

    if (
      !isAssignedToStudent(
        test,
        email
      )
    ) {
      return forbidden(
        "This test is not assigned to this student."
      );
    }

    console.log(
      "Checking existing result..."
    );

    const existingResult =
      await getResult(
        testId,
        email
      );

    if (existingResult) {
      return ok({
        success: true,
        alreadyCompleted: true,
        attendance: null,
        result:
          sanitizeResult(
            existingResult
          )
      });
    }

    console.log(
      "Checking if test is live..."
    );

    if (
      !isCurrentlyLive(test)
    ) {
      return forbidden(
        "This test is not currently live."
      );
    }

    console.log(
      "Collecting student details..."
    );

    const studentDetails = {};

    [
      "name",
      "email",
      "school",
      "regNo",
      "rollNumber",
      "registerNumber",
      "collegeName"
    ].forEach(
      field => {
        const value =
          stringValue(
            body[field]
          );

        if(value){
          studentDetails[field] =
            value;
        }
      }
    );

    if(
      body &&
      typeof body === "object"
    ){
      Object.keys(body)
        .filter(
          key =>
            key.startsWith(
              "custom_"
            )
        )
        .forEach(
          key => {
            const value =
              stringValue(
                body[key]
              );

            if(value){
              studentDetails[key] =
                value;
            }
          }
        );
    }

    console.log(
      "Student details collected:",
      Object.keys(studentDetails)
    );

    console.log(
      "Starting attendance..."
    );

    const attendance =
      await startAttendance(
        test,
        email,
        studentName,
        studentDetails
      );

    console.log(
      "Attendance started successfully"
    );

    return ok({
      success: true,
      alreadyStarted:
        Boolean(
          attendance.alreadyStarted
        ),
      attendance
    });

  } catch (error) {
    console.error(
      "handleStartAttendance error:",
      error
    );

    console.error(
      "Error stack:",
      error?.stack
    );

    return serverError(
      error
    );
  }
}

// ------------------------------------------------------------
// HEARTBEAT
// ------------------------------------------------------------

async function handleHeartbeat(
  event
) {
  const body =
    parseBody(event);

  const email =
    getStudentEmail(
      event,
      body
    );

  const testId =
    stringValue(
      body.testId
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const attendance =
    await getAttendance(
      testId,
      email
    );

  if (!attendance) {
    return notFound(
      "Attendance record not found."
    );
  }

  const updated =
    await heartbeatAttendance(
      attendance
    );

  return ok({
    success: true,
    attendance:
      updated
  });
}

// ------------------------------------------------------------
// GET ATTENDANCE
// ------------------------------------------------------------

async function handleGetAttendance(
  event,
  testId
) {
  const email =
    getStudentEmail(
      event
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const attendance =
    await getAttendance(
      testId,
      email
    );

  return ok({
    success: true,
    attendance:
      attendance || null
  });
}

// ------------------------------------------------------------
// SUBMIT TEST
// ------------------------------------------------------------

async function submitTest(
  event
) {
  try {
    console.log(
      "submitTest called"
    );

    const body =
      parseBody(event);

  const email =
    getStudentEmail(
      event,
      body
    );

  const testId =
    stringValue(
      body.testId
    );

  const studentName =
    stringValue(
      body.studentName ||
      body.name
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const test =
    await getTest(testId);

  if (!test) {
    return notFound(
      "Test not found."
    );
  }

  if (
    !isAssignedToStudent(
      test,
      email
    )
  ) {
    return forbidden(
      "This test is not assigned to this student."
    );
  }

  const existingResult =
    await getResult(
      testId,
      email
    );

  if (existingResult) {
    return ok({
      success: true,
      alreadySubmitted: true,
      result:
        sanitizeResult(
          existingResult
        )
    });
  }

  const studentDetails = {};

  [
    "name",
    "email",
    "school",
    "regNo",
    "rollNumber",
    "registerNumber",
    "collegeName"
  ].forEach(
    field => {
      const value =
        stringValue(
          body[field]
        );

      if(value){
        studentDetails[field] =
          value;
      }
    }
  );

  if(
    body &&
    typeof body === "object"
  ){
    Object.keys(body)
      .filter(
        key =>
          key.startsWith(
            "custom_"
          )
      )
      .forEach(
        key => {
          const value =
            stringValue(
              body[key]
            );

          if(value){
            studentDetails[key] =
              value;
          }
        }
      );
  }

  let attendance =
    await getAttendance(
      testId,
      email
    );

  if (!attendance) {
    if (
      !isCurrentlyLive(test)
    ) {
      return forbidden(
        "The test has ended."
      );
    }

    attendance =
      await startAttendance(
        test,
        email,
        studentName,
        studentDetails
      );
  }

  const nowMs =
    Date.now();

  const attemptEndMs =
    toMillis(
      attendance.attemptEndTime
    );

  // Small server grace period for network delay.
  const GRACE_MS =
    30 * 1000;

  if (
    Number.isFinite(
      attemptEndMs
    ) &&
    nowMs >
      attemptEndMs +
        GRACE_MS
  ) {
    return forbidden(
      "The allowed test time has expired."
    );
  }

  const questions =
    normalizeQuestions(
      test.questions
    );

  const answers =
    normalizeAnswers(
      body.answers
    );

  let correct = 0;
  let wrong = 0;
  let unanswered = 0;
  let score = 0;

  for (const question of questions) {
    const selected =
      cleanAnswer(
        answers[
          question.id
        ]
      );

    const correctAnswer =
      cleanAnswer(
        question.answer
      );

    if (!selected) {
      unanswered++;
      continue;
    }

    if (
      selected ===
      correctAnswer
    ) {
      correct++;
      score +=
        safeNumber(
          question.marks,
          1
        );
    } else {
      wrong++;
    }
  }

  const totalQuestions =
    questions.length;

  const totalMarks =
    calculateTotalMarks(
      questions
    );

  const percentage =
    totalMarks > 0
      ? roundNumber(
          (score /
            totalMarks) *
            100,
          2
        )
      : 0;

  const submittedAt =
    nowIso();

  const result = {
    resultId:
      makeResultId(
        testId,
        email
      ),

    testId,

    testCode:
      test.testCode,

    testTitle:
      test.title,

    studentEmail:
      email,

    studentName:
      studentName ||
      attendance.studentName ||
      "",

    ...(
      studentDetails &&
      typeof studentDetails === "object"
        ? studentDetails
        : {}
    ),

    score,

    totalMarks,

    percentage,

    correct,

    wrong,

    unanswered,

    totalQuestions,

    submittedAt,

    startedAt:
      attendance.startedAt,

    attemptEndTime:
      attendance.attemptEndTime,

    status:
      "COMPLETED",

    // Keep answers in DynamoDB.
    // sanitizeResult never exposes these to the student.
    answers
  };

  await saveResult(
    result
  );

  // Mark attendance submitted.
  await dynamo.send(
    new UpdateItemCommand({
      TableName:
        ATTENDANCE_TABLE,

      Key: marshall({
        attendanceId:
          attendance.attendanceId
      }),

      UpdateExpression:
        "SET #status = :status, submittedAt = :submittedAt, updatedAt = :updatedAt",

      ExpressionAttributeNames: {
        "#status":
          "status"
      },

      ExpressionAttributeValues:
        marshall({
          ":status":
            "SUBMITTED",

          ":submittedAt":
            submittedAt,

          ":updatedAt":
            submittedAt
        })
    })
  );

  return ok({
    success: true,
    alreadySubmitted: false,
    result:
      sanitizeResult(
        result
      )
  });

  } catch (error) {
    console.error(
      "submitTest error:",
      error
    );

    console.error(
      "Error stack:",
      error?.stack
    );

    return serverError(
      error
    );
  }
}

// ------------------------------------------------------------
// GET TEST QUESTIONS
// ------------------------------------------------------------

async function getTestQuestions(
  event,
  testId
) {
  try {
    console.log(
      "getTestQuestions called with testId:",
      testId
    );

    const email =
      getStudentEmail(
        event
      );

    console.log(
      "Student email:",
      email
    );

    if (!email) {
      return badRequest(
        "studentEmail is required."
      );
    }

    if (!testId) {
      return badRequest(
        "testId is required."
      );
    }

    console.log(
      "Fetching test:",
      testId
    );

    const test =
      await getTest(testId);

    console.log(
      "Test found:",
      test ? "yes" : "no"
    );

    if (!test) {
      return notFound(
        "Test not found."
      );
    }

    console.log(
      "Test assignment type:",
      test.assignmentType
    );

    console.log(
      "Checking assignment for email:",
      email
    );

    const isAssigned =
      isAssignedToStudent(
        test,
        email
      );

    console.log(
      "Is assigned:",
      isAssigned
    );

    if (!isAssigned) {
      return forbidden(
        "This test is not assigned to this student."
      );
    }

    const isLive =
      isCurrentlyLive(test);

    console.log(
      "Is currently live:",
      isLive
    );

    if (!isLive) {
      return forbidden(
        "This test is not currently live."
      );
    }

    console.log(
      "Normalizing questions, count:",
      test.questions?.length || 0
    );

    const questions =
      normalizeQuestions(
        test.questions || []
      );

    console.log(
      "Questions normalized:",
      questions.length
    );

    return ok({
      success: true,
      questions:
        sanitizeQuestions(
          questions
        )
    });

  } catch (error) {
    console.error(
      "getTestQuestions error:",
      error
    );

    console.error(
      "Error stack:",
      error?.stack
    );

    return serverError(
      error
    );
  }
}

// ------------------------------------------------------------
// STUDENT RESULT
// ------------------------------------------------------------

async function getStudentResult(
  event,
  testId
) {
  const email =
    getStudentEmail(
      event
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const result =
    await getResult(
      testId,
      email
    );

  if (!result) {
    return notFound(
      "Result not found."
    );
  }

  return ok({
    success: true,
    completed: true,
    result:
      sanitizeResult(
        result
      )
  });
}

// ------------------------------------------------------------
// ALL RESULTS
// ------------------------------------------------------------

async function allResults(
  event
) {
  let results =
    await getAllResults();

  const query =
    getQuery(event);

  const testId =
    stringValue(
      query?.testId
    );

  const studentEmail =
    normalizeEmail(
      query?.studentEmail
    );

  // Filter by testId if provided
  if(testId){
    results =
      results.filter(
        r =>
          String(
            r.testId || ""
          ) ===
          String(testId)
      );
  }

  // Filter by studentEmail if provided
  if(studentEmail){
    results =
      results.filter(
        r =>
          normalizeEmail(
            r.studentEmail || ""
          ) ===
          studentEmail
      );
  }

  results.sort(
    (a, b) =>
      String(
        b.submittedAt || ""
      ).localeCompare(
        String(
          a.submittedAt || ""
        )
      )
  );

  return ok({
    success: true,
    results:
      results.map(
        sanitizeResult
      ),
    total:
      results.length
  });
}

// ------------------------------------------------------------
// DETAILED REPORT RESULT
//
// Admin/report endpoint only.
//
// This is NOT used by student result endpoint.
// Therefore correct answers and selected answers are not
// exposed to normal students.
// ------------------------------------------------------------

function buildDetailedReportResult(
  test,
  result,
  profile = null,
  attendance = null
) {
  const questions =
    normalizeQuestions(
      test.questions
    );

  const answers =
    normalizeAnswers(
      result.answers
    );

  const questionDetails =
    questions.map(
      (question, index) => {
        const selectedAnswer =
          cleanAnswer(
            answers[
              question.id
            ]
          );

        const correctAnswer =
          cleanAnswer(
            question.answer
          );

        let status =
          "UNANSWERED";

        let scoreEarned = 0;

        if (
          selectedAnswer
        ) {
          if (
            selectedAnswer ===
            correctAnswer
          ) {
            status =
              "CORRECT";

            scoreEarned =
              safeNumber(
                question.marks,
                1
              );
          } else {
            status =
              "WRONG";
          }
        }

        return {
          questionNumber:
            index + 1,

          questionId:
            question.id,

          question:
            question.question,

          options:
            Array.isArray(
              question.options
            )
              ? question.options
              : [],

          selectedAnswer:
            selectedAnswer ||
            null,

          correctAnswer:
            correctAnswer ||
            null,

          status,

          marks:
            safeNumber(
              question.marks,
              1
            ),

          scoreEarned:
            scoreEarned
        };
      }
    );

  const correct =
    questionDetails.filter(
      q =>
        q.status ===
        "CORRECT"
    ).length;

  const wrong =
    questionDetails.filter(
      q =>
        q.status ===
        "WRONG"
    ).length;

  const unanswered =
    questionDetails.filter(
      q =>
        q.status ===
        "UNANSWERED"
    ).length;

  const totalMarks =
    calculateTotalMarks(
      questions
    );

  const score =
    questionDetails.reduce(
      (sum, q) =>
        sum +
        safeNumber(
          q.scoreEarned,
          0
        ),
      0
    );

  const percentage =
    totalMarks > 0
      ? roundNumber(
          (score /
            totalMarks) *
            100,
          2
        )
      : 0;

  return {
    resultId:
      result.resultId,

    testId:
      result.testId,

    testCode:
      result.testCode,

    testTitle:
      test.title,

    studentEmail:
      normalizeEmail(
        result.studentEmail
      ),

    studentName:
      result.studentName ||
      profile?.studentName ||
      profile?.name ||
      "",

    ...extractStudentDetails(result),

    score,

    totalMarks,

    percentage,

    correct,

    wrong,

    unanswered,

    totalQuestions:
      questions.length,

    status:
      "COMPLETED",

    startedAt:
      result.startedAt ||
      attendance?.startedAt ||
      "",

    attemptEndTime:
      result.attemptEndTime ||
      attendance?.attemptEndTime ||
      "",

    submittedAt:
      result.submittedAt ||
      "",

    attendanceStatus:
      attendance?.status ||
      "SUBMITTED",

    lastHeartbeatAt:
      attendance?.lastHeartbeatAt ||
      "",

    questionDetails
  };
}

// ------------------------------------------------------------
// FULL TEST REPORT
// ------------------------------------------------------------

async function reportForTest(
  testId
) {
  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const test =
    await getTest(testId);

  if (!test) {
    return notFound(
      "Test not found."
    );
  }

  const [
    allResults,
    allStudents,
    allAttendance
  ] = await Promise.all([
    getAllResults(),
    getAllStudents(),
    getAllAttendance()
  ]);

  const assignedEmails =
    getAssignedEmails(test);

  // ----------------------------------------------------------
  // Student profile map
  // ----------------------------------------------------------

  const studentMap =
    new Map();

  for (
    const student
    of allStudents
  ) {
    const email =
      normalizeEmail(
        student.studentEmail ||
        student.email
      );

    if (email) {
      studentMap.set(
        email,
        student
      );
    }
  }

  // ----------------------------------------------------------
  // Results for this test
  // ----------------------------------------------------------

  const testResults =
    allResults.filter(
      result =>
        String(
          result.testId
        ) ===
        String(testId)
    );

  const resultMap =
    new Map();

  for (
    const result
    of testResults
  ) {
    const email =
      normalizeEmail(
        result.studentEmail
      );

    if (email) {
      resultMap.set(
        email,
        result
      );
    }
  }

  // ----------------------------------------------------------
  // Attendance map
  // ----------------------------------------------------------

  const attendanceMap =
    new Map();

  for (
    const attendance
    of allAttendance
  ) {
    if (
      String(
        attendance.testId
      ) !==
      String(testId)
    ) {
      continue;
    }

    const email =
      normalizeEmail(
        attendance.studentEmail
      );

    if (email) {
      attendanceMap.set(
        email,
        attendance
      );
    }
  }

  // ----------------------------------------------------------
  // BUILD ASSIGNED ROSTER
  //
  // Specific email assignment:
  //   exactly those email addresses.
  //
  // ALL STUDENTS:
  //   all registered MCQStudents.
  //
  // For ALL STUDENTS, also include any result email that
  // exists, so a submitted student can never disappear from
  // the report due to a missing/old profile record.
  // ----------------------------------------------------------

  let rosterEmails = [];

  if (
    assignedEmails.length
  ) {
    rosterEmails = [
      ...assignedEmails
    ];
  } else {
    rosterEmails = [
      ...studentMap.keys()
    ];

    for (
      const email
      of resultMap.keys()
    ) {
      if (
        !rosterEmails.includes(
          email
        )
      ) {
        rosterEmails.push(
          email
        );
      }
    }

    for (
      const email
      of attendanceMap.keys()
    ) {
      if (
        !rosterEmails.includes(
          email
        )
      ) {
        rosterEmails.push(
          email
        );
      }
    }
  }

  rosterEmails = [
    ...new Set(
      rosterEmails
        .map(
          normalizeEmail
        )
        .filter(Boolean)
    )
  ];

  // ----------------------------------------------------------
  // INDIVIDUAL STUDENT REPORT
  // ----------------------------------------------------------

  const students = [];

  let totalCorrect = 0;
  let totalWrong = 0;
  let totalUnanswered = 0;
  let totalScore = 0;

  let completedCount = 0;
  let inProgressCount = 0;
  let notStartedCount = 0;

  const completedPercentages = [];

  for (
    const email
    of rosterEmails
  ) {
    const profile =
      studentMap.get(
        email
      );

    const result =
      resultMap.get(
        email
      );

    const attendance =
      attendanceMap.get(
        email
      );

    let status =
      "NOT_STARTED";

    let completed =
      false;

    if (result) {
      status =
        "COMPLETED";

      completed =
        true;

      completedCount++;
    } else if (
      attendance &&
      attendance.status ===
        "IN_PROGRESS"
    ) {
      status =
        "IN_PROGRESS";

      inProgressCount++;
    } else {
      status =
        "NOT_STARTED";

      notStartedCount++;
    }

    const studentName =
      stringValue(
        result?.studentName
      ) ||
      stringValue(
        profile?.studentName
      ) ||
      stringValue(
        profile?.name
      ) ||
      email;

    if (result) {
      const detailed =
        buildDetailedReportResult(
          test,
          result,
          profile,
          attendance
        );

      totalCorrect +=
        detailed.correct;

      totalWrong +=
        detailed.wrong;

      totalUnanswered +=
        detailed.unanswered;

      totalScore +=
        detailed.score;

      completedPercentages.push(
        detailed.percentage
      );

      students.push({
        assigned: true,

        completed: true,

        status:
          "COMPLETED",

        studentEmail:
          email,

        studentName,

        ...extractStudentDetails(detailed),

        score:
          detailed.score,

        totalMarks:
          detailed.totalMarks,

        percentage:
          detailed.percentage,

        correct:
          detailed.correct,

        wrong:
          detailed.wrong,

        unanswered:
          detailed.unanswered,

        totalQuestions:
          detailed.totalQuestions,

        startedAt:
          detailed.startedAt,

        attemptEndTime:
          detailed.attemptEndTime,

        submittedAt:
          detailed.submittedAt,

        attendanceStatus:
          detailed.attendanceStatus,

        lastHeartbeatAt:
          detailed.lastHeartbeatAt,

        questionDetails:
          detailed.questionDetails
      });
    } else {
      students.push({
        assigned: true,

        completed: false,

        status,

        studentEmail:
          email,

        studentName,

        ...extractStudentDetails(
          attendance || profile || {}
        ),

        score: 0,

        totalMarks:
          calculateTotalMarks(
            test.questions
          ),

        percentage: 0,

        correct: 0,

        wrong: 0,

        unanswered:
          normalizeQuestions(
            test.questions
          ).length,

        totalQuestions:
          normalizeQuestions(
            test.questions
          ).length,

        startedAt:
          attendance?.startedAt ||
          "",

        attemptEndTime:
          attendance?.attemptEndTime ||
          "",

        submittedAt: "",

        attendanceStatus:
          attendance?.status ||
          "NOT_STARTED",

        lastHeartbeatAt:
          attendance?.lastHeartbeatAt ||
          "",

        questionDetails: []
      });
    }
  }

  const totalAssignedStudents =
    students.length;

  const notAttempted =
    Math.max(
      0,
      totalAssignedStudents -
        completedCount
    );

  const averagePercentage =
    completedPercentages.length
      ? roundNumber(
          completedPercentages.reduce(
            (sum, value) =>
              sum + value,
            0
          ) /
            completedPercentages.length,
          2
        )
      : 0;

  const completionRate =
    totalAssignedStudents > 0
      ? roundNumber(
          (completedCount /
            totalAssignedStudents) *
            100,
          2
        )
      : 0;

  const totalQuestions =
    normalizeQuestions(
      test.questions
    ).length;

  const totalPossibleMarks =
    calculateTotalMarks(
      test.questions
    );

  // ----------------------------------------------------------
  // Extra results not in assigned roster.
  //
  // Normally this should be empty.
  // Useful for identifying old/incorrect result records.
  // ----------------------------------------------------------

  const assignedSet =
    new Set(
      rosterEmails
    );

  const unassignedResults =
    testResults
      .filter(
        result =>
          !assignedSet.has(
            normalizeEmail(
              result.studentEmail
            )
          )
      )
      .map(
        result =>
          sanitizeResult(
            result
          )
      );

  return ok({
    success: true,

    test: {
      testId:
        test.testId,

      testCode:
        test.testCode,

      title:
        test.title,

      totalQuestions,

      totalMarks:
        totalPossibleMarks,

      durationMinutes:
        safeNumber(
          test.durationMinutes,
          0
        ),

      liveHours:
        safeNumber(
          test.liveHours,
          calculateLiveHours(
            test.startTime,
            test.endTime
          )
        ),

      startTime:
        test.startTime || "",

      endTime:
        test.endTime || "",

      status:
        getTestStatus(test),

      published:
        test.published === true,

      active:
        test.active !== false,

      assignmentType:
        assignedEmails.length
          ? "SPECIFIC_STUDENTS"
          : "ALL_STUDENTS",

      assignedEmails:
        assignedEmails
    },

    report: {
      totalAssignedStudents,

      completed:
        completedCount,

      attempted:
        completedCount,

      inProgress:
        inProgressCount,

      notStarted:
        notStartedCount,

      notAttempted,

      completionRate,

      averagePercentage,

      totalCorrect,

      totalWrong,

      totalUnanswered,

      totalScore,

      totalPossibleMarks:
        totalPossibleMarks,

      students,

      results:
        students.filter(
          student =>
            student.completed
        ),

      unassignedResults
    }
  });
}

// ------------------------------------------------------------
// LEGACY REPORT ROUTE
// ------------------------------------------------------------

async function legacyReport(
  testId
) {
  return reportForTest(
    testId
  );
}

// ------------------------------------------------------------
// SAVE / UPDATE STUDENT PROFILE
// ------------------------------------------------------------

async function saveStudentProfile(body) {
  try {
    const email = normalizeEmail(body.studentEmail || body.email);
    if (!email) {
      return badRequest("studentEmail is required.");
    }

    // Find existing by email
    const existing = await findStudentByEmail(email);

    const studentId = existing?.studentId || body.studentId || makeId("student");
    const createdAt = existing?.createdAt || nowIso();
    const updatedAt = nowIso();

    const student = {
      ...(existing || {}),
      studentId,
      studentEmail: email,
      email,
      studentName: stringValue(body.studentName || body.name || existing?.studentName || existing?.name),
      name: stringValue(body.name || body.studentName || existing?.name || existing?.studentName),
      phone: stringValue(body.phone || existing?.phone),
      rollNumber: stringValue(body.rollNumber || existing?.rollNumber),
      regNo: stringValue(body.regNo || existing?.regNo),
      collegeName: stringValue(body.collegeName || existing?.collegeName),
      profileComplete: body.profileComplete !== undefined ? Boolean(body.profileComplete) : (existing?.profileComplete || false),
      createdAt,
      updatedAt
    };

    await dynamo.send(
      new PutItemCommand({
        TableName: STUDENTS_TABLE,
        Item: marshall(student, { removeUndefinedValues: true })
      })
    );

    return ok({
      success: true,
      message: "Student profile saved successfully.",
      student
    });
  } catch (error) {
    console.error("saveStudentProfile error:", error);
    return serverError(error);
  }
}

// ------------------------------------------------------------
// GET CREATOR PROFILE
// ------------------------------------------------------------

async function getCreatorProfile(event) {
  const query = getQuery(event);
  const email = normalizeEmail(query?.creatorEmail);

  if (!email) {
    return badRequest("creatorEmail is required.");
  }

  const creator = await findCreatorByEmail(email);

  if (!creator) {
    return notFound("Creator profile not found.");
  }

  return ok({
    success: true,
    creator
  });
}

// ------------------------------------------------------------
// SAVE / UPDATE CREATOR PROFILE
// ------------------------------------------------------------

async function saveCreatorProfile(body) {
  try {
    const email = normalizeEmail(body.creatorEmail || body.email);
    if (!email) {
      return badRequest("creatorEmail is required.");
    }

    // Find existing by email
    const existing = await findCreatorByEmail(email);

    const creatorId = existing?.creatorId || body.creatorId || makeId("creator");
    const createdAt = existing?.createdAt || nowIso();
    const updatedAt = nowIso();

    const creator = {
      ...(existing || {}),
      creatorId,
      creatorEmail: email,
      email,
      creatorName: stringValue(body.creatorName || body.name || existing?.creatorName || existing?.name),
      name: stringValue(body.name || body.creatorName || existing?.name || existing?.creatorName),
      phone: stringValue(body.phone || existing?.phone),
      orgType: stringValue(body.orgType || existing?.orgType),
      orgName: stringValue(body.orgName || existing?.orgName),
      studentCount: stringValue(body.studentCount || existing?.studentCount),
      profileComplete: body.profileComplete !== undefined ? Boolean(body.profileComplete) : (existing?.profileComplete || false),
      createdAt,
      updatedAt
    };

    await dynamo.send(
      new PutItemCommand({
        TableName: CREATORS_TABLE,
        Item: marshall(creator, { removeUndefinedValues: true })
      })
    );

    return ok({
      success: true,
      message: "Creator profile saved successfully.",
      creator
    });
  } catch (error) {
    console.error("saveCreatorProfile error:", error);
    return serverError(error);
  }
}

// ------------------------------------------------------------
// GET STUDENT PROFILE
// ------------------------------------------------------------

async function getStudentProfile(
  event
) {
  const email =
    getStudentEmail(
      event
    );

  if (!email) {
    return badRequest(
      "studentEmail is required."
    );
  }

  const student =
    await getStudent(
      email
    );

  if (!student) {
    return notFound(
      "Student profile not found."
    );
  }

  return ok({
    success: true,
    student
  });
}

// ------------------------------------------------------------
// LEGACY QUESTIONS ROUTE
// ------------------------------------------------------------

async function legacyQuestions(
  event
) {
  const query =
    getQuery(event);

  const testId =
    stringValue(
      query.testId
    );

  if (!testId) {
    return badRequest(
      "testId is required."
    );
  }

  const test =
    await getTest(testId);

  if (!test) {
    return notFound(
      "Test not found."
    );
  }

  return ok({
    success: true,
    questions:
      sanitizeQuestions(
        test.questions
      )
  });
}

// ------------------------------------------------------------
// ATTEMPT STATUS
// ------------------------------------------------------------

async function getAttemptStatus(event) {
  try {
    const query = getQuery(event);
    const testId = stringValue(query.testId);
    const email = normalizeEmail(query.studentEmail || query.email);

    if (!testId || !email) {
      return badRequest("testId and studentEmail are required.");
    }

    const attempt = await getAttempt(testId, email);

    if (!attempt) {
      return ok({
        success: true,
        status: "NOT_STARTED",
        violationCount: 0,
        malpracticeReason: null
      });
    }

    return ok({
      success: true,
      status: attempt.status || "ACTIVE",
      violationCount: attempt.violationCount || 0,
      malpracticeReason: attempt.malpracticeReason || null,
      malpracticeTimestamp: attempt.malpracticeTimestamp || null
    });
  } catch (error) {
    console.error("getAttemptStatus error:", error);
    return ok({
      success: true,
      status: "NOT_STARTED",
      violationCount: 0,
      malpracticeReason: null
    });
  }
}

// ------------------------------------------------------------
// RECORD VIOLATION
// ------------------------------------------------------------

async function recordAttemptViolation(
  event
) {
  try {
    const body =
      parseBody(event);

    const testId =
      stringValue(
        body.testId
      );

    const email =
      normalizeEmail(
        body.studentEmail ||
        body.email
      );

    if (!testId || !email) {
      return badRequest(
        "testId and studentEmail are required."
      );
    }

    const attempt =
      await getOrCreateAttempt(
        testId,
        email
      );

    if (!attempt) {
      return serverError(
        new Error(
          "Failed to get or create attempt."
        )
      );
    }

    const violationType =
      stringValue(
        body.violationType ||
        body.type
      ) ||
      "UNKNOWN";

    const questionIndex =
      safeNumber(
        body.questionIndex,
        0
      );

    const timestamp =
      stringValue(
        body.timestamp
      ) ||
      nowIso();

    const violation = {
      type:
        violationType,
      questionIndex,
      timestamp,
      details:
        body.details || {}
    };

    const violations =
      Array.isArray(
        attempt.violations
      )
        ? [
            ...attempt.violations,
            violation
          ]
        : [violation];

    const violationCount =
      violations.length;

    const updated = {
      ...attempt,
      violationCount,
      violations,
      updatedAt:
        nowIso()
    };

    await saveAttempt(
      updated
    );

    return ok({
      success: true,
      violationCount
    });
  } catch (error) {
    console.error(
      "recordAttemptViolation error:",
      error
    );

    return ok({
      success: true,
      violationCount: 0
    });
  }
}

// ------------------------------------------------------------
// MARK MALPRACTICE
// ------------------------------------------------------------

async function markMalpractice(
  event
) {
  try {
    const body =
      parseBody(event);

    const testId =
      stringValue(
        body.testId
      );

    const email =
      normalizeEmail(
        body.studentEmail ||
        body.email
      );

    if (!testId || !email) {
      return badRequest(
        "testId and studentEmail are required."
      );
    }

    const attempt =
      await getOrCreateAttempt(
        testId,
        email
      );

    if (!attempt) {
      return serverError(
        new Error(
          "Failed to get or create attempt."
        )
      );
    }

    const reason =
      stringValue(
        body.reason
      ) ||
      "Multiple exam security violations";

    const timestamp =
      stringValue(
        body.timestamp
      ) ||
      nowIso();

    const updated = {
      ...attempt,
      status:
        "MALPRACTICE",
      malpracticeReason:
        reason,
      malpracticeTimestamp:
        timestamp,
      malpracticeQuestionIndex:
        safeNumber(
          body.questionIndex,
          0
        ),
      currentAnswers:
        body.currentAnswers ||
        [],
      updatedAt:
        nowIso()
    };

    await saveAttempt(
      updated
    );

    return ok({
      success: true,
      status:
        "MALPRACTICE"
    });
  } catch (error) {
    console.error(
      "markMalpractice error:",
      error
    );

    return ok({
      success: true,
      status:
        "MALPRACTICE"
    });
  }
}

// ------------------------------------------------------------
// RESUME ATTEMPT
// ------------------------------------------------------------

async function resumeAttempt(
  event
) {
  try {
    const body =
      parseBody(event);

    const testId =
      stringValue(
        body.testId
      );

    const email =
      normalizeEmail(
        body.studentEmail ||
        body.email
      );

    const teacherId =
      stringValue(
        body.teacherId
      );

    if (
      !testId ||
      !email ||
      !teacherId
    ) {
      return badRequest(
        "testId, studentEmail, and teacherId are required."
      );
    }

    const test =
      await getTest(testId);

    if (!test) {
      return notFound(
        "Test not found."
      );
    }

    const attempt =
      await getAttempt(
        testId,
        email
      );

    if (!attempt) {
      return notFound(
        "Attempt not found."
      );
    }

    if (
      attempt.status !==
      "MALPRACTICE"
    ) {
      return badRequest(
        "Attempt is not in MALPRACTICE status."
      );
    }

    const updated = {
      ...attempt,
      status:
        "RESUMED",
      violationCount: 0,
      resumedBy:
        teacherId,
      resumedAt:
        nowIso(),
      updatedAt:
        nowIso()
    };

    await saveAttempt(
      updated
    );

    return ok({
      success: true,
      status:
        "RESUMED"
    });
  } catch (error) {
    console.error(
      "resumeAttempt error:",
      error
    );

    return serverError(
      error
    );
  }
}

// ------------------------------------------------------------
// HEALTH
// ------------------------------------------------------------

async function health() {
  return ok({
    success: true,
    message:
      "Quizora Lambda is running.",
    region: REGION,
    tables: {
      tests:
        TESTS_TABLE,
      questions:
        QUESTIONS_TABLE,
      results:
        RESULTS_TABLE,
      students:
        STUDENTS_TABLE,
      attendance:
        ATTENDANCE_TABLE,
      attempts:
        ATTEMPTS_TABLE
    },
    time:
      nowIso()
  });
}

// ------------------------------------------------------------
// MAIN HANDLER
// ------------------------------------------------------------

export const handler =
  async event => {
    try {
      console.log(
        "REQUEST:",
        JSON.stringify({
          method:
            getMethod(event),
          path:
            normalizePath(event),
          query:
            getQuery(event)
        })
      );

      const method =
        getMethod(event);

      const path =
        normalizePath(event);

      // ------------------------------------------------------
      // CORS PREFLIGHT
      // ------------------------------------------------------

      if (
        method === "OPTIONS"
      ) {
        return response(
          204,
          ""
        );
      }

      // ------------------------------------------------------
      // HEALTH
      // ------------------------------------------------------

      if (
        method === "GET" &&
        path === "/health"
      ) {
        return health();
      }

      // ------------------------------------------------------
      // CREATOR - LIST TESTS
      //
      // GET /tests
      // GET /api/tests
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path === "/tests" ||
          path === "/api/tests"
        )
      ) {
        return listCreatorTests();
      }

      // ------------------------------------------------------
      // CREATOR - CREATE TEST
      //
      // POST /tests
      // POST /api/tests
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path === "/tests" ||
          path === "/api/tests"
        )
      ) {
        return createTest(
          parseBody(event)
        );
      }

      // ------------------------------------------------------
      // CREATOR - ALL RESULTS
      //
      // GET /results
      // GET /api/results
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path === "/results" ||
          path === "/api/results"
        )
      ) {
        return allResults(
          event
        );
      }

      // ------------------------------------------------------
      // CREATOR - REPORT
      //
      // GET /tests/:testId/report
      // GET /api/tests/:testId/report
      // GET /reports/:testId
      // GET /api/reports/:testId
      // ------------------------------------------------------

      let match =
        path.match(
          /^\/(?:api\/)?tests\/([^/]+)\/report$/i
        );

      if (
        method === "GET" &&
        match
      ) {
        return reportForTest(
          decodeURIComponent(
            match[1]
          )
        );
      }

      match =
        path.match(
          /^\/(?:api\/)?reports\/([^/]+)$/i
        );

      if (
        method === "GET" &&
        match
      ) {
        return legacyReport(
          decodeURIComponent(
            match[1]
          )
        );
      }

      // ------------------------------------------------------
      // CREATOR - EDIT TIME
      //
      // PUT /tests/:testId/time
      // PUT /api/tests/:testId/time
      // PATCH same
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?tests\/([^/]+)\/time$/i
        );

      if (
        (
          method === "PUT" ||
          method === "PATCH"
        ) &&
        match
      ) {
        return updateTest(
          decodeURIComponent(
            match[1]
          ),
          parseBody(event)
        );
      }

      // ------------------------------------------------------
      // CREATOR - EDIT TEST
      //
      // PUT /tests/:testId
      // PATCH /tests/:testId
      // PUT /api/tests/:testId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?tests\/([^/]+)$/i
        );

      if (
        (
          method === "PUT" ||
          method === "PATCH"
        ) &&
        match
      ) {
        return updateTest(
          decodeURIComponent(
            match[1]
          ),
          parseBody(event)
        );
      }

      // ------------------------------------------------------
      // CREATOR - DELETE TEST
      //
      // DELETE /tests/:testId
      // DELETE /api/tests/:testId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?tests\/([^/]+)$/i
        );

      if (
        method === "DELETE" &&
        match
      ) {
        const testId =
          decodeURIComponent(
            match[1]
          );

        const test =
          await getTest(
            testId
          );

        if (!test) {
          return notFound(
            "Test not found."
          );
        }

        await deleteTestById(
          testId
        );

        return ok({
          success: true,
          message:
            "Test deleted successfully.",
          testId
        });
      }

      // ------------------------------------------------------
      // STUDENT TEST LIST
      //
      // GET /student/tests
      // GET /api/student/tests
      // GET /student-tests
      // GET /api/student-tests
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/student/tests" ||
          path ===
            "/api/student/tests" ||
          path ===
            "/student-tests" ||
          path ===
            "/api/student-tests"
        )
      ) {
        return listStudentTests(
          event
        );
      }

      // ------------------------------------------------------
      // STUDENT SINGLE TEST
      //
      // GET /student/test/:testId
      // GET /api/student/test/:testId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?student\/test\/([^/]+)$/i
        );

      if (
        method === "GET" &&
        match
      ) {
        return getStudentTest(
          event,
          decodeURIComponent(
            match[1]
          )
        );
      }

      // ------------------------------------------------------
      // TEST BY CODE
      //
      // GET /test-code/:code
      // GET /api/test-code/:code
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?test-code\/([^/]+)$/i
        );

      if (
        method === "GET" &&
        match
      ) {
        return getTestByCode(
          event,
          decodeURIComponent(
            match[1]
          )
        );
      }

      // ------------------------------------------------------
      // STUDENT RESULT
      //
      // GET /student/result/:testId
      // GET /api/student/result/:testId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?student\/result\/([^/]+)$/i
        );

      if (
        method === "GET" &&
        match
      ) {
        return getStudentResult(
          event,
          decodeURIComponent(
            match[1]
          )
        );
      }

      // ------------------------------------------------------
      // ATTENDANCE START
      //
      // POST /attendance
      // POST /api/attendance
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/attendance" ||
          path ===
            "/api/attendance"
        )
      ) {
        return handleStartAttendance(
          event
        );
      }

      // ------------------------------------------------------
      // ATTENDANCE HEARTBEAT
      //
      // POST /attendance/heartbeat
      // PUT /attendance/heartbeat
      // PATCH /attendance/heartbeat
      // ------------------------------------------------------

      if (
        (
          method === "POST" ||
          method === "PUT" ||
          method === "PATCH"
        ) &&
        (
          path ===
            "/attendance/heartbeat" ||
          path ===
            "/api/attendance/heartbeat"
        )
      ) {
        return handleHeartbeat(
          event
        );
      }

      // ------------------------------------------------------
      // GET ATTENDANCE
      //
      // GET /attendance/:testId
      // GET /api/attendance/:testId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?attendance\/([^/]+)$/i
        );

      if (
        method === "GET" &&
        match
      ) {
        return handleGetAttendance(
          event,
          decodeURIComponent(
            match[1]
          )
        );
      }

      // ------------------------------------------------------
      // ATTENDANCE UPDATE
      //
      // PUT /attendance
      // PATCH /attendance
      // ------------------------------------------------------

      if (
        (
          method === "PUT" ||
          method === "PATCH"
        ) &&
        (
          path ===
            "/attendance" ||
          path ===
            "/api/attendance"
        )
      ) {
        return handleHeartbeat(
          event
        );
      }

      // ------------------------------------------------------
      // SUBMIT TEST
      //
      // POST /student/submit
      // POST /api/student/submit
      // POST /submit-test
      // POST /api/submit-test
      // POST /results
      // POST /api/results
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/student/submit" ||
          path ===
            "/api/student/submit" ||
          path ===
            "/submit-test" ||
          path ===
            "/api/submit-test" ||
          path ===
            "/results" ||
          path ===
            "/api/results"
        )
      ) {
        return submitTest(
          event
        );
      }

      // ------------------------------------------------------
      // STUDENT PROFILE
      //
      // POST /students
      // POST /api/students
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/students" ||
          path ===
            "/api/students"
        )
      ) {
        return saveStudentProfile(
          parseBody(event)
        );
      }

      // ------------------------------------------------------
      // GET ALL STUDENTS (Admin)
      //
      // GET /students/all
      // GET /api/students/all
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/students/all" ||
          path ===
            "/api/students/all"
        )
      ) {
        const students =
          await getAllStudents();

        return ok({
          success: true,
          students,
          total: students.length
        });
      }

      // ------------------------------------------------------
      // GET STUDENT PROFILE
      //
      // GET /students
      // GET /api/students
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/students" ||
          path ===
            "/api/students"
        )
      ) {
        return getStudentProfile(
          event
        );
      }

      // ------------------------------------------------------
      // DELETE STUDENT
      //
      // DELETE /students/:studentId
      // DELETE /api/students/:studentId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?students\/([^/]+)$/i
        );

      if (
        method === "DELETE" &&
        match
      ) {
        const studentId =
          decodeURIComponent(
            match[1]
          );

        try {
          // Delete from DynamoDB
          await dynamo.send(
            new DeleteItemCommand({
              TableName:
                STUDENTS_TABLE,
              Key: marshall({
                studentId
              })
            })
          );

          return ok({
            success: true,
            message:
              "Student deleted successfully.",
            studentId
          });

        } catch (error) {
          console.error(
            "Delete student error:",
            error
          );

          return serverError(
            error
          );
        }
      }

      // ------------------------------------------------------
      // GET CREATOR PROFILE
      //
      // GET /creators?creatorEmail=...
      // GET /api/creators?creatorEmail=...
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/creators" ||
          path ===
            "/api/creators"
        )
      ) {
        return getCreatorProfile(
          event
        );
      }

      // ------------------------------------------------------
      // SAVE CREATOR PROFILE
      //
      // POST /creators
      // POST /api/creators
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/creators" ||
          path ===
            "/api/creators"
        )
      ) {
        return saveCreatorProfile(
          parseBody(event)
        );
      }

      // ------------------------------------------------------
      // GET ALL CREATORS (Admin)
      //
      // GET /creators/all
      // GET /api/creators/all
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/creators/all" ||
          path ===
            "/api/creators/all"
        )
      ) {
        const creators =
          await getAllCreators();

        return ok({
          success: true,
          creators,
          total: creators.length
        });
      }

      // ------------------------------------------------------
      // DELETE CREATOR
      //
      // DELETE /creators/:creatorId
      // DELETE /api/creators/:creatorId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?creators\/([^/]+)$/i
        );

      if (
        method === "DELETE" &&
        match
      ) {
        const creatorId =
          decodeURIComponent(
            match[1]
          );

        try {
          // Delete from DynamoDB
          await dynamo.send(
            new DeleteItemCommand({
              TableName:
                CREATORS_TABLE,
              Key: marshall({
                creatorId
              })
            })
          );

          return ok({
            success: true,
            message:
              "Creator deleted successfully.",
            creatorId
          });

        } catch (error) {
          console.error(
            "Delete creator error:",
            error
          );

          return serverError(
            error
          );
        }
      }

      // ------------------------------------------------------
      // GET QUESTIONS BY TEST ID
      //
      // GET /questions/:testId
      // GET /api/questions/:testId
      // ------------------------------------------------------

      match =
        path.match(
          /^\/(?:api\/)?questions\/([^/]+)$/i
        );

      console.log(
        "Questions route match:",
        match ? "yes" : "no",
        "path:",
        path
      );

      if (
        method === "GET" &&
        match
      ) {
        console.log(
          "Calling getTestQuestions with testId:",
          match[1]
        );

        return getTestQuestions(
          event,
          decodeURIComponent(
            match[1]
          )
        );
      }

      // ------------------------------------------------------
      // LEGACY QUESTIONS
      //
      // GET /questions
      // GET /api/questions
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/questions" ||
          path ===
            "/api/questions"
        )
      ) {
        return legacyQuestions(
          event
        );
      }

      // ------------------------------------------------------
      // GET ATTEMPT STATUS
      //
      // GET /attempt/status
      // GET /api/attempt/status
      // ------------------------------------------------------

      if (
        method === "GET" &&
        (
          path ===
            "/attempt/status" ||
          path ===
            "/api/attempt/status"
        )
      ) {
        return getAttemptStatus(
          event
        );
      }

      // ------------------------------------------------------
      // RECORD VIOLATION
      //
      // POST /attempt/violation
      // POST /api/attempt/violation
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/attempt/violation" ||
          path ===
            "/api/attempt/violation"
        )
      ) {
        return recordAttemptViolation(
          event
        );
      }

      // ------------------------------------------------------
      // MARK MALPRACTICE
      //
      // POST /attempt/malpractice
      // POST /api/attempt/malpractice
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/attempt/malpractice" ||
          path ===
            "/api/attempt/malpractice"
        )
      ) {
        return markMalpractice(
          event
        );
      }

      // ------------------------------------------------------
      // RESUME ATTEMPT
      //
      // POST /attempt/resume
      // POST /api/attempt/resume
      // ------------------------------------------------------

      if (
        method === "POST" &&
        (
          path ===
            "/attempt/resume" ||
          path ===
            "/api/attempt/resume"
        )
      ) {
        return resumeAttempt(
          event
        );
      }

      // ------------------------------------------------------
      // 404
      // ------------------------------------------------------

      return notFound(
        `Route not found: ${method} ${path}`
      );

    } catch (error) {
      return serverError(
        error
      );
    }
  };

