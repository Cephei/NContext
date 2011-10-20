// --------------------------------------------------------------------------------------------------------------------
// <copyright file="EfGenericRepository.cs">
//   This file is part of NContext.
//
//   NContext is free software: you can redistribute it and/or modify
//   it under the terms of the GNU General Public License as published by
//   the Free Software Foundation, either version 3 of the License, or any later version.
//
//   NContext is distributed in the hope that it will be useful,
//   but WITHOUT ANY WARRANTY; without even the implied warranty of
//   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//   GNU General Public License for more details.
//
//   You should have received a copy of the GNU General Public License
//   along with NContext.  If not, see <http://www.gnu.org/licenses/>.
// </copyright>
//
// <summary>
//   Defines a entity framework implementation of the repository pattern for database communication.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Validation;
using System.Linq;
using System.Linq.Expressions;

using NContext.Application.Domain;
using NContext.Application.Domain.Specifications;

namespace NContext.Persistence.EntityFramework
{
    /// <summary>
    /// Defines a generic implementation of the repository pattern for database communication.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    public class EfGenericRepository<TEntity> : IEfGenericRepository<TEntity> where TEntity : class, IEntity
    {
        #region Fields

        private readonly DbContext _Context;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="EfGenericRepository{TEntity}"/> class.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <remarks></remarks>
        protected internal EfGenericRepository(DbContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException("context");
            }

            if (UnitOfWorkController.AmbientUnitOfWork == null)
            {
                throw new Exception("A repository must be created within the scope of an existing IUnitOfWork instance.");
            }

            _Context = context;
        }

        #endregion

        #region Implementation of IQueryable

        /// <summary>
        /// Gets the type of the element(s) that are returned when the expression tree associated with this instance of <see cref="IQueryable" /> is executed.
        /// </summary>
        /// <returns>
        /// A <see cref="Type" /> that represents the type of the element(s) that are returned when the expression tree associated with this object is executed.
        /// </returns>
        public Type ElementType
        {
            get { return RepositoryQuery.ElementType; }
        }

        /// <summary>
        /// Gets the expression tree that is associated with the instance of <see cref="IQueryable" />.
        /// </summary>
        /// <returns>
        /// The <see cref="Expression" /> that is associated with this instance of <see cref="IQueryable" />.
        /// </returns>
        public Expression Expression
        {
            get { return RepositoryQuery.Expression; }
        }

        /// <summary>
        /// Gets the query provider that is associated with this data source.
        /// </summary>
        /// <returns>
        /// The <see cref="IQueryProvider" /> that is associated with this data source.
        /// </returns>
        public IQueryProvider Provider
        {
            get { return RepositoryQuery.Provider; }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="IEnumerator{TEntity}" /> that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<TEntity> GetEnumerator()
        {
            return RepositoryQuery.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="IEnumerator" /> object that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return RepositoryQuery.GetEnumerator();
        }

        #region Implementation of IEfGenericRepository<TEntity>

        /// <summary>
        /// Gets the <see cref="IQueryable{T}"/> used by <see cref="IEfGenericRepository{TEntity}"/> 
        /// to execute Linq queries.
        /// </summary>
        /// <value>A <see cref="IQueryable{TEntity}"/> instance.</value>
        /// <remarks>
        /// Inheritors of this base class should return a valid non-null <see cref="IQueryable{TEntity}"/> instance.
        /// </remarks>
        protected IQueryable<TEntity> RepositoryQuery
        {
            get
            {
                return _Context.Set<TEntity>();
            }
        }

        /// <summary>
        /// Adds a transient instance of <typeparamref cref="TEntity"/> to the unit of work
        /// to be persisted and inserted by the repository.
        /// </summary>
        /// <param name="entity">An instance of <typeparamref name="TEntity"/> that should be
        /// inserted into the database.</param>
        /// <remarks>Implementors of this method must handle the PersistAdd scenario.</remarks>
        public void Add(TEntity entity)
        {
            _Context.Set<TEntity>().Add(entity);
        }

        /// <summary>
        /// Adds a transient instance of <typeparamref cref="TEntity"/> to the unit of work
        /// to be persisted and deleted by the repository.
        /// </summary>
        /// <param name="entity">An instance of <typeparamref name="TEntity"/> that should be
        /// deleted from the database.</param>
        /// <remarks>Implementors of this method must handle the PersistDelete scneario.</remarks>
        public void Remove(TEntity entity)
        {
            _Context.Set<TEntity>().Remove(entity);
        }

        /// <summary>
        /// Attaches a detached entity.
        /// </summary>
        /// <param name="entity">The entity instance to attach back to the repository.</param>
        public void Attach(TEntity entity)
        {
            _Context.Set<TEntity>().Attach(entity);
        }

        /// <summary>
        /// Refreshes a entity instance.
        /// </summary>
        /// <param name="entity">The entity to refresh.</param>
        public void Refresh(TEntity entity)
        {
            _Context.Entry<TEntity>(entity).Reload();
        }

        /// <summary>
        /// Queries the context based on the provided specification and returns results that
        /// are only satisfied by the specification.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="specification">A <see cref="SpecificationBase{TEntity}"/> instance used to filter results
        /// that only satisfy the specification.</param>
        /// <returns>
        /// A <see cref="IQueryable{TEntity}"/> that can be used to enumerate over the results
        /// of the query.
        /// </returns>
        public IQueryable<TEntity> AllMatching(SpecificationBase<TEntity> specification)
        {
            return _Context.Set<TEntity>().Where(specification.IsSatisfiedBy()).AsQueryable();
        }

        /// <summary>
        /// Gets the results for the specified paged query.
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="pageIndex">Index of the page.</param>
        /// <param name="pageCount">The page count.</param>
        /// <param name="orderByExpression">The order by expression.</param>
        /// <param name="ascending">if set to <c>true</c> [ascending].</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public IQueryable<TEntity> GetPaged<TProperty>(Int32 pageIndex, Int32 pageCount, Expression<Func<TEntity, TProperty>> orderByExpression, Boolean ascending = true)
        {
            var set = _Context.Set<TEntity>();
            if (ascending)
            {
                return set.OrderBy(orderByExpression)
                    .Skip(pageCount * pageIndex)
                    .Take(pageCount)
                    .AsQueryable();
            }

            return set.OrderByDescending(orderByExpression)
                .Skip(pageCount * pageIndex)
                .Take(pageCount)
                .AsQueryable();
        }

        /// <summary>
        /// Validates the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public DbEntityValidationResult Validate(TEntity entity)
        {
            return _Context.Entry<TEntity>(entity).GetValidationResult();
        }

        /// <summary>
        /// Queries the <see cref="DbContext"/> via SQL, using the specified parameters.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public IQueryable<TEntity> SqlQuery(String sql, params Object[] parameters)
        {
            return _Context.Set<TEntity>().SqlQuery(sql, parameters).AsQueryable();
        }

        #endregion

        #endregion

        #region Implementation of IDisposable

        /// <summary>
        /// Releases unmanaged resources and performs other cleanup operations before 
        /// the <see cref="EfGenericRepository{TEntity}"/> is reclaimed by garbage collection.
        /// </summary>
        /// <remarks></remarks>
        ~EfGenericRepository()
        {
            Dispose(false);
        }

        /// <summary>
        /// Gets a value indicating whether this instance is disposed.
        /// </summary>
        /// <remarks></remarks>
        protected Boolean IsDisposed { get; private set; }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposeManagedResources"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(Boolean disposeManagedResources)
        {
            if (IsDisposed)
            {
                return;
            }

            if (disposeManagedResources)
            {
            }

            IsDisposed = true;
        }

        #endregion
    }
}