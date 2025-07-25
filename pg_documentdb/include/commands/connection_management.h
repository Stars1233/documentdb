/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * include/commands/connection_management.h
 *
 * Functions and callbacks related with connection management.
 *
 *-------------------------------------------------------------------------
 */
#include <libpq-fe.h>

#ifndef CONNECTION_MANAGEMENT_H
#define CONNECTION_MANAGEMENT_H


/*
 * Function that needs to be called via abort handler.
 */
void ConnMgrTryCancelActiveConnection(void);

/*
 * Functions internally used by ExtensionExecuteQueryViaLibPQ to let the
 * connection manager know about the active libpq connection.
 */
void ConnMgrResetActiveConnection(PGconn *conn);
void ConnMgrForgetActiveConnection(void);

/*
 * Functions used to manage a PG connection's state and report possible errors.
 */
bool PGConnXactIsActive(PGconn *conn);
bool PGConnTryCancel(PGconn *conn);

void PGConnReportError(PGconn *conn, PGresult *result, int elevel);
void PGConnFinishConnectionEstablishment(PGconn *conn);

#endif
