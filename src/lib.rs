use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use deadpool::managed::{Metrics, Object, RecycleError, RecycleResult, Timeouts};
use deadpool::Status;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::pool::{ConnectionGuard, ConnectionManager, Pool};
use rbdc::Error;
use rbs::value::map::ValueMap;
use rbs::{value, Value};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct DeadPool {
    pub manager: ConnManagerProxy,
    pub inner: deadpool::managed::Pool<ConnManagerProxy>,
}

unsafe impl Send for DeadPool {}

unsafe impl Sync for DeadPool {}

impl Debug for DeadPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").finish()
    }
}

impl DeadPool {
    /// Retrieves Status of this Pool.
    pub fn status(&self) -> Status {
        self.inner.status()
    }
}

#[async_trait]
impl Pool for DeadPool {
    fn new(manager: ConnectionManager) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(Self {
            manager: ConnManagerProxy {
                inner: manager.clone(),
                conn: None,
            },
            inner: deadpool::managed::Pool::builder(ConnManagerProxy {
                inner: manager,
                conn: None,
            })
            // .runtime(Runtime::Tokio1)
            // .create_timeout(Some(Duration::from_secs(30)))
            .build()
            .map_err(|e| Error::from(e.to_string()))?,
        })
    }

    async fn get(&self) -> Result<Box<dyn Connection>, Error> {
        let v = self
            .inner
            .get()
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        let conn = ConnManagerProxy {
            inner: v.manager_proxy.clone(),
            conn: Some(v),
        };
        Ok(Box::new(conn))
    }

    async fn get_timeout(&self, d: Duration) -> Result<Box<dyn Connection>, Error> {
        let out = Timeouts {
            create: Some(d),
            wait: Some(d),
            recycle: None,
        };
        let v = self
            .inner
            .timeout_get(&out)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        let conn = ConnManagerProxy {
            inner: v.manager_proxy.clone(),
            conn: Some(v),
        };
        Ok(Box::new(conn))
    }

    async fn set_conn_max_lifetime(&self, _max_lifetime: Option<Duration>) {
        //un impl
    }

    async fn set_max_idle_conns(&self, _n: u64) {
        //un impl
    }

    async fn set_max_open_conns(&self, n: u64) {
        self.inner.resize(n as usize)
    }

    fn driver_type(&self) -> &str {
        self.manager.inner.driver_type()
    }

    async fn state(&self) -> Value {
        let mut m = ValueMap::with_capacity(10);
        let state = self.status();
        m.insert(value!("max_size"), value!(state.max_size));
        m.insert(value!("size"), value!(state.size));
        m.insert(value!("available"), value!(state.available));
        m.insert(value!("waiting"), value!(state.waiting));
        Value::Map(m)
    }
}

pub struct ConnManagerProxy {
    pub inner: ConnectionManager,
    pub conn: Option<Object<ConnManagerProxy>>,
}

impl From<ConnectionManager> for ConnManagerProxy {
    fn from(value: ConnectionManager) -> Self {
        ConnManagerProxy {
            inner: value,
            conn: None,
        }
    }
}

impl deadpool::managed::Manager for ConnManagerProxy {
    type Type = ConnectionGuard;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        self.inner.connect().await
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        if obj.conn.is_none() {
            return Err(RecycleError::Message("none".into()));
        }
        self.inner.check(obj).await?;
        Ok(())
    }
}

impl Connection for ConnManagerProxy {
    fn get_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<Vec<Box<dyn Row>>, Error>> {
        if self.conn.is_none() {
            return Box::pin(async {
                Err::<Vec<Box<dyn Row>>, Error>(Error::from("conn is drop"))
            });
        }
        self.conn.as_mut().unwrap().get_rows(sql, params)
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<ExecResult, Error>> {
        if self.conn.is_none() {
            return Box::pin(async { Err(Error::from("conn is drop")) });
        }
        self.conn.as_mut().unwrap().exec(sql, params)
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        if self.conn.is_none() {
            return Box::pin(async { Err(Error::from("conn is drop")) });
        }
        Box::pin(async { self.conn.as_mut().unwrap().ping().await })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        if self.conn.is_none() {
            return Box::pin(async { Err(Error::from("conn is drop")) });
        }
        Box::pin(async { self.conn.as_mut().unwrap().close().await })
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        if self.conn.is_none() {
            return Box::pin(async { Err(Error::from("conn is drop")) });
        }
        self.conn.as_mut().unwrap().begin()
    }

    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        if self.conn.is_none() {
            return Box::pin(async { Err(Error::from("conn is drop")) });
        }
        self.conn.as_mut().unwrap().commit()
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        if self.conn.is_none() {
            return Box::pin(async { Err(Error::from("conn is drop")) });
        }
        self.conn.as_mut().unwrap().rollback()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
